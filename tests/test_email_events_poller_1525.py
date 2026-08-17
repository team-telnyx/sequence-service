"""REVOPS-1525 — Email API events pull-poller (pivot from webhook push).

The Ed25519 webhook receiver (PR #30) is live but Telnyx cannot reach the host
(no public ingress — corporate tailnet, Funnel not permitted). This pivot
PULLS delivery events from GET /v2/email/events on a ~2-minute interval and
feeds them through the SAME ``process_email_event`` the receiver uses, so
bounce/complaint/unsubscribe/delivered handling is identical between the
push and pull paths.

These tests prove:
  - ``extract_event_from_api_item`` parses real Telnyx events-API list items
    into the shared ``EmailEvent`` (handled types only; queued/sending/sent/
    opened/clicked are skipped — the send path owns those).
  - ``poll_once`` walks pages from a persisted cursor, processes handled
    events via ``process_email_event``, and advances the cursor ONLY after
    every event on a page succeeds. A failed page leaves the cursor untouched
    so the next run re-fetches and retries (dedupe markers make re-processing
    a no-op).
  - A bounce event pulled via the poller produces the SAME durable side
    effects as the receiver path: ``Suppression`` with ``API_BOUNCE``,
    ``SequenceEnrollmentStep.status == BOUNCED``,
    ``SequenceEnrollment.status == BOUNCED`` (mirrors
    ``test_bounce_event_suppresses_and_marks_step_bounced`` in the receiver
    suite).
  - Double-run is a no-op: re-pulling an already-processed event returns
    ``already_processed`` and writes no duplicate suppression.
  - API failures (401/5xx/network) log a warning, leave the cursor untouched,
    and never crash the caller.

httpx is mocked at the transport level via ``httpx.MockTransport`` — no
network, no real Telnyx calls. Real event payload shapes come from the live
API probe (2026-08-17): ``{data: [{id, event_type, occurred_at, payload:
{id, status, from, to, subject, ...}}], meta: {page_size, page_cursor}}``.
"""

from datetime import datetime

import httpx
import pytest
from sqlalchemy import select

from src.models.models import (
    EmailEventsPollerCursor,
    EnrollmentStatus,
    EnrollmentStepStatus,
    ProcessedEmailEvent,
    SequenceEnrollment,
    SequenceEnrollmentStep,
    SentEmail,
    Suppression,
    SuppressionReason,
)
from src.services.email_events import (
    EVENT_BOUNCE,
    EVENT_COMPLAINT,
    EVENT_DELIVERED,
    EVENT_UNSUBSCRIBE,
)
from src.services.email_events_poller import (
    POLLER_FEED,
    extract_event_from_api_item,
    poll_once,
    load_cursor,
    save_cursor,
)


# ── realistic Telnyx events-API item builder ─────────────────────────────────


def _api_item(
    event_type="email.delivered",
    event_id="evt-001",
    message_id="msg-001",
    to_email="vp@acme.com",
    occurred_at="2026-08-17T10:00:00Z",
    status="delivered",
):
    """Build one ``data`` list item as the live GET /v2/email/events API
    returns it (verified 2026-08-17). The per-item shape mirrors the webhook
    envelope's ``data`` object (same fields: id, event_type, occurred_at,
    payload.{id, status, from, to})."""
    return {
        "id": event_id,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "payload": {
            "id": message_id,
            "status": status,
            "from": "quinn.c@telnyx.com",
            "to": [to_email],
            "subject": "Hi",
        },
    }


def _page(items, page_size=25, next_cursor=None):
    """Build a full API response page."""
    return {
        "data": items,
        "meta": {
            "page_size": page_size,
            "time_range": None,
            "page_cursor": next_cursor,
        },
    }


def _transport(pages_by_cursor):
    """Build an httpx.MockTransport that returns pages keyed by the
    ``page[cursor]`` query param. ``None`` key = first page (no cursor)."""

    def handler(request):
        cursor = request.url.params.get("page[cursor]")
        # Sanity: the poller must always request page[size]=25.
        assert request.url.params.get("page[size]") is not None, (
            "poller must send page[size]"
        )
        # Auth header must be present on every call.
        assert request.headers.get("Authorization", "").startswith("Bearer "), (
            "poller must send a Bearer Authorization header"
        )
        page = pages_by_cursor.get(cursor)
        if page is None:
            # Unknown cursor → return an empty short page (treated as done).
            return httpx.Response(200, json=_page([]))
        return httpx.Response(200, json=page)

    return httpx.MockTransport(handler)


# ── seed helper (mirrors the receiver test's _seed_sent_email) ───────────────


async def _seed_sent_email(
    session_factory,
    seeded,
    message_id="msg-poll-001",
    contact_email="vpoll@acme.com",
    enrollment_id="enr-poll",
    step_id="estep-poll",
):
    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id=enrollment_id,
                sequence_id=seeded["sequence_id"],
                mailbox_id=seeded["active_mailbox_id"],
                contact_email=contact_email,
                contact_name="VPoll",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=1,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id=step_id,
                enrollment_id=enrollment_id,
                step_id="step-1",
                mailbox_id=seeded["active_mailbox_id"],
                status=EnrollmentStepStatus.SENT,
                custom_subject="Hi",
                custom_body="<p>Body</p>",
            )
        )
        s.add(
            SentEmail(
                id="sent-" + step_id,
                message_id=message_id,
                mailbox_id=seeded["active_mailbox_id"],
                enrollment_step_id=step_id,
                subject="Hi",
                body="<p>Body</p>",
                to_email=contact_email,
                from_email="quinn.c@telnyx.com",
                sent_at=datetime.utcnow(),
            )
        )
        await s.commit()
    return {"enrollment_id": enrollment_id, "step_id": step_id}


BASE_URL = "https://api.telnyx.com/v2"
API_KEY = "test-email-api-key"


# ── extract_event_from_api_item ──────────────────────────────────────────────


class TestExtractEvent:
    def test_delivered(self):
        item = _api_item(
            event_type="email.delivered", event_id="evt-d", message_id="msg-d"
        )
        ev = extract_event_from_api_item(item)
        assert ev is not None
        assert ev.event_type == EVENT_DELIVERED
        assert ev.event_id == "evt-d"
        assert ev.message_id == "msg-d"
        assert ev.to_email == "vp@acme.com"

    def test_bounce(self):
        item = _api_item(
            event_type="email.bounced", event_id="evt-b", message_id="msg-b"
        )
        ev = extract_event_from_api_item(item)
        assert ev is not None
        assert ev.event_type == EVENT_BOUNCE

    def test_complaint(self):
        item = _api_item(
            event_type="email.complained", event_id="evt-c", message_id="msg-c"
        )
        ev = extract_event_from_api_item(item)
        assert ev is not None
        assert ev.event_type == EVENT_COMPLAINT

    def test_unsubscribe(self):
        item = _api_item(
            event_type="email.unsubscribed", event_id="evt-u", message_id="msg-u"
        )
        ev = extract_event_from_api_item(item)
        assert ev is not None
        assert ev.event_type == EVENT_UNSUBSCRIBE

    def test_skips_queued_sending_sent(self):
        """queued/sending/sent are owned by the send path — the poller must
        NOT process them (would double-count send-state the send path already
        owns)."""
        for raw in ("email.queued", "email.sending", "email.sent"):
            assert extract_event_from_api_item(_api_item(event_type=raw)) is None, (
                f"{raw} must be skipped by the poller (send-path owned)"
            )

    def test_skips_opened_clicked(self):
        """opened/clicked are engagement events — not in the poller's scope
        (bounce/complaint/unsubscribe/delivered only)."""
        for raw in ("email.opened", "email.clicked"):
            assert extract_event_from_api_item(_api_item(event_type=raw)) is None

    def test_skips_suppressed(self):
        """Review round 1: ``suppressed`` is the receiver's job (the poller
        must not double-suppress) — removed from ``_HANDLED_INTERNAL_TYPES``.
        Pre-fix the poller would re-process ``email.suppressed`` events and
        double-write suppressions alongside the receiver path."""
        assert (
            extract_event_from_api_item(_api_item(event_type="email.suppressed"))
            is None
        )

    def test_unknown_event_type_returns_none(self):
        assert (
            extract_event_from_api_item(_api_item(event_type="email.unknown")) is None
        )

    def test_handles_scalar_to_field(self):
        """The events API may return ``to`` as a scalar OR a list (the
        webhook envelope handles both; the poller must too)."""
        item = _api_item()
        item["payload"]["to"] = "scalar@acme.com"
        ev = extract_event_from_api_item(item)
        assert ev is not None
        assert ev.to_email == "scalar@acme.com"

    def test_empty_to_list_yields_empty_string(self):
        item = _api_item()
        item["payload"]["to"] = []
        ev = extract_event_from_api_item(item)
        assert ev is not None
        assert ev.to_email == ""

    def test_malformed_item_missing_event_type_raises(self):
        """A malformed item (missing required fields) raises — the poller
        catches and skips without crashing the run."""
        item = {
            "id": "x",
            "occurred_at": "2026-08-17T10:00:00Z",
            "payload": {"id": "m"},
        }
        with pytest.raises(KeyError):
            extract_event_from_api_item(item)

    def test_malformed_item_missing_payload_id_raises(self):
        item = _api_item()
        del item["payload"]["id"]
        with pytest.raises(KeyError):
            extract_event_from_api_item(item)


# ── cursor persistence ──────────────────────────────────────────────────────


class TestCursorPersistence:
    @pytest.mark.asyncio
    async def test_load_cursor_returns_none_when_empty(self, session_factory):
        assert await load_cursor(session_factory) is None

    @pytest.mark.asyncio
    async def test_save_then_load_roundtrip(self, session_factory):
        await save_cursor(session_factory, POLLER_FEED, "cursor-abc")
        assert await load_cursor(session_factory) == "cursor-abc"

    @pytest.mark.asyncio
    async def test_save_cursor_overwrites_existing(self, session_factory):
        await save_cursor(session_factory, POLLER_FEED, "cursor-1")
        await save_cursor(session_factory, POLLER_FEED, "cursor-2")
        assert await load_cursor(session_factory) == "cursor-2"
        async with session_factory() as s:
            rows = (await s.execute(select(EmailEventsPollerCursor))).scalars().all()
            assert len(rows) == 1, "save must upsert, not insert a second row"


# ── poll_once — happy paths mirroring the receiver suite ─────────────────────


class TestPollOnceHappyPaths:
    @pytest.mark.asyncio
    async def test_bounce_writes_suppression_and_marks_step_bounced(
        self, session_factory, seeded
    ):
        """The poller's bounce path must produce the SAME durable side
        effects as the receiver's ``test_bounce_event_suppresses_and_marks_step_bounced``:
        Suppression(API_BOUNCE), step BOUNCED, enrollment BOUNCED."""
        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-poll-bounce",
            contact_email="pollbounce@acme.com",
            enrollment_id="enr-poll-bounce",
            step_id="estep-poll-bounce",
        )

        page = _page(
            [
                _api_item(
                    event_type="email.bounced",
                    event_id="evt-poll-bounce",
                    message_id="msg-poll-bounce",
                    to_email="pollbounce@acme.com",
                    status="bounced",
                )
            ]
        )
        # Short page (1 item < page_size 25) → poll stops after this page.
        client = httpx.AsyncClient(transport=_transport({None: page}))

        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.processed == 1
        assert summary.errors == 0
        # Tail page (1 item < page_size 25) with no page_cursor → no non-null
        # cursor saved → cursor_advanced False (REVOPS-1525 follow-up: pre-fix
        # this was True because the poller persisted None unconditionally).
        assert summary.cursor_advanced is False

        async with session_factory() as s:
            supps = (await s.execute(select(Suppression))).scalars().all()
            assert len(supps) == 1
            assert supps[0].reason == SuppressionReason.API_BOUNCE
            assert supps[0].email == "pollbounce@acme.com"
            assert supps[0].source_enrollment_id == ids["enrollment_id"]

            step = await s.get(SequenceEnrollmentStep, ids["step_id"])
            assert step.status == EnrollmentStepStatus.BOUNCED
            enr = await s.get(SequenceEnrollment, ids["enrollment_id"])
            assert enr.status == EnrollmentStatus.BOUNCED

            marker = (
                await s.execute(
                    select(ProcessedEmailEvent).where(
                        ProcessedEmailEvent.id == "evt-poll-bounce"
                    )
                )
            ).scalar_one_or_none()
            assert marker is not None, "dedupe marker must be written"

    @pytest.mark.asyncio
    async def test_delivered_sets_delivered_at_no_suppression(
        self, session_factory, seeded
    ):
        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-poll-delivered",
            contact_email="polldeliv@acme.com",
            enrollment_id="enr-poll-delivered",
            step_id="estep-poll-delivered",
        )

        page = _page(
            [
                _api_item(
                    event_type="email.delivered",
                    event_id="evt-poll-delivered",
                    message_id="msg-poll-delivered",
                    to_email="polldeliv@acme.com",
                    status="delivered",
                )
            ]
        )
        client = httpx.AsyncClient(transport=_transport({None: page}))
        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.processed == 1
        assert summary.errors == 0

        async with session_factory() as s:
            assert len((await s.execute(select(Suppression))).scalars().all()) == 0
            sent = await s.get(SentEmail, "sent-" + ids["step_id"])
            assert sent.delivered_at is not None, "delivered_at must be set"
            enr = await s.get(SequenceEnrollment, ids["enrollment_id"])
            assert enr.status == EnrollmentStatus.ACTIVE, (
                "delivered must NOT change enrollment status"
            )

    @pytest.mark.asyncio
    async def test_complaint_suppresses_api_complaint(self, session_factory, seeded):
        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-poll-complaint",
            contact_email="pollcomp@acme.com",
            enrollment_id="enr-poll-complaint",
            step_id="estep-poll-complaint",
        )

        page = _page(
            [
                _api_item(
                    event_type="email.complained",
                    event_id="evt-poll-complaint",
                    message_id="msg-poll-complaint",
                    to_email="pollcomp@acme.com",
                    status="complained",
                )
            ]
        )
        client = httpx.AsyncClient(transport=_transport({None: page}))
        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.processed == 1, f"complaint must process, got {summary}"

        async with session_factory() as s:
            supps = (await s.execute(select(Suppression))).scalars().all()
            assert len(supps) == 1
            assert supps[0].reason == SuppressionReason.API_COMPLAINT
            enr = await s.get(SequenceEnrollment, ids["enrollment_id"])
            assert enr.status == EnrollmentStatus.UNSUBSCRIBED


# ── poll_once — dedupe / double-run no-op ─────────────────────────────────────


class TestPollOnceDedupe:
    @pytest.mark.asyncio
    async def test_double_run_is_no_op(self, session_factory, seeded):
        """Re-pulling an already-processed event is a no-op: the marker catches
        it (already_processed), no duplicate suppression, cursor unchanged
        on the second run (short page, same cursor)."""
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-poll-dedupe",
            contact_email="polldedupe@acme.com",
            enrollment_id="enr-poll-dedupe",
            step_id="estep-poll-dedupe",
        )

        bounce_item = _api_item(
            event_type="email.bounced",
            event_id="evt-poll-dedupe",
            message_id="msg-poll-dedupe",
            to_email="polldedupe@acme.com",
            status="bounced",
        )
        page = _page([bounce_item])
        client = httpx.AsyncClient(transport=_transport({None: page}))

        first = await poll_once(session_factory, client, BASE_URL, API_KEY)
        assert first.processed == 1

        # Second run returns the SAME page (simulating a re-fetch before the
        # cursor advanced — e.g. a crash between save_cursor and the next
        # fetch, or a manual cursor reset). The marker makes it a no-op.
        second = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert second.already_processed == 1, (
            f"second run must be already_processed, got {second}"
        )
        assert second.processed == 0
        assert second.errors == 0

        async with session_factory() as s:
            supps = (await s.execute(select(Suppression))).scalars().all()
            assert len(supps) == 1, "no duplicate suppression on re-pull"
            markers = (
                (
                    await s.execute(
                        select(ProcessedEmailEvent).where(
                            ProcessedEmailEvent.id == "evt-poll-dedupe"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(markers) == 1, "no duplicate marker on re-pull"


# ── poll_once — cursor advancement contract ─────────────────────────────────


class TestPollOnceCursorContract:
    @pytest.mark.asyncio
    async def test_cursor_advances_only_after_successful_page(
        self, session_factory, seeded, monkeypatch
    ):
        """A failed event mid-page must NOT advance the cursor past
        unprocessed events. We simulate a processing failure by patching
        ``process_email_event`` to raise for one event_id; the cursor must
        stay at the page's start cursor."""
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-poll-fail",
            contact_email="pollfail@acme.com",
            enrollment_id="enr-poll-fail",
            step_id="estep-poll-fail",
        )

        good_item = _api_item(
            event_type="email.delivered",
            event_id="evt-poll-fail-ok",
            message_id="msg-poll-fail",
            to_email="pollfail@acme.com",
            status="delivered",
        )
        bad_item = _api_item(
            event_type="email.bounced",
            event_id="evt-poll-fail-bad",
            message_id="msg-poll-fail",
            to_email="pollfail@acme.com",
            status="bounced",
        )
        page = _page([good_item, bad_item], next_cursor="next-cursor-xyz")
        # Key the page to the PRE-SEEDED cursor so the poller (which resumes
        # from "pre-existing-cursor") fetches this page, not an empty default.
        client = httpx.AsyncClient(transport=_transport({"pre-existing-cursor": page}))

        # Pre-seed a cursor so we can assert it does NOT change.
        await save_cursor(session_factory, POLLER_FEED, "pre-existing-cursor")

        import src.services.email_events_poller as poller_mod

        original = poller_mod.process_email_event

        async def flaky(db, event):
            if event.event_id == "evt-poll-fail-bad":
                raise RuntimeError("simulated processing failure")
            return await original(db, event)

        monkeypatch.setattr(poller_mod, "process_email_event", flaky)

        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.errors >= 1, f"expected at least one error, got {summary}"
        assert summary.cursor_advanced is False
        # Cursor must be UNCHANGED — the failed event blocks advancement.
        assert await load_cursor(session_factory) == "pre-existing-cursor"

    @pytest.mark.asyncio
    async def test_mid_page_failure_stops_page_and_retries_on_next_run(
        self, session_factory, seeded, monkeypatch
    ):
        """Review round 1 repro: 3-event page, event #2 raises → #1 marker
        written, #2 AND #3 unprocessed, cursor unchanged. Next run: #1
        no-ops (marker), #2 and #3 process. Pre-fix the loop ``continue``d
        to #3 after #2 failed, processing #3 and masking the failure
        boundary — the reviewer required STOP-at-first-failure so the
        invariant "events at or after the cursor are unprocessed" holds."""
        # 3 independent chains so the 3 events don't interact (a delivered
        # for msg-A, a delivered for msg-B, a delivered for msg-C).
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-mid-1",
            contact_email="mid1@acme.com",
            enrollment_id="enr-mid-1",
            step_id="estep-mid-1",
        )
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-mid-2",
            contact_email="mid2@acme.com",
            enrollment_id="enr-mid-2",
            step_id="estep-mid-2",
        )
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-mid-3",
            contact_email="mid3@acme.com",
            enrollment_id="enr-mid-3",
            step_id="estep-mid-3",
        )

        item1 = _api_item(
            event_type="email.delivered",
            event_id="evt-mid-1",
            message_id="msg-mid-1",
            to_email="mid1@acme.com",
        )
        item2 = _api_item(
            event_type="email.delivered",
            event_id="evt-mid-2",
            message_id="msg-mid-2",
            to_email="mid2@acme.com",
        )
        item3 = _api_item(
            event_type="email.delivered",
            event_id="evt-mid-3",
            message_id="msg-mid-3",
            to_email="mid3@acme.com",
        )
        page = _page([item1, item2, item3], next_cursor="next-mid")
        client = httpx.AsyncClient(transport=_transport({None: page}))

        import src.services.email_events_poller as poller_mod

        original = poller_mod.process_email_event
        bad_calls = {"n": 0}

        async def flaky(db, event):
            # Fail evt-mid-2 ONLY on the first call (run 1). On retry the
            # bad_calls counter has advanced, so #2 processes cleanly.
            if event.event_id == "evt-mid-2" and bad_calls["n"] == 0:
                bad_calls["n"] += 1
                raise RuntimeError("simulated processing failure on first run")
            return await original(db, event)

        monkeypatch.setattr(poller_mod, "process_email_event", flaky)

        # Run 1: #1 processes, #2 raises → STOP, #3 unprocessed, cursor unchanged.
        run1 = await poll_once(session_factory, client, BASE_URL, API_KEY)
        assert run1.processed == 1, f"run1: #1 must process, got {run1}"
        assert run1.errors == 1, f"run1: #2 must error, got {run1}"
        assert run1.cursor_advanced is False
        assert await load_cursor(session_factory) is None

        async with session_factory() as s:
            marker_ids = {
                m.id
                for m in (await s.execute(select(ProcessedEmailEvent))).scalars().all()
            }
        assert "evt-mid-1" in marker_ids, "#1 marker must be written on run 1"
        assert "evt-mid-2" not in marker_ids, "#2 must be unprocessed (no marker)"
        assert "evt-mid-3" not in marker_ids, "#3 must be unprocessed (no marker)"

        # Run 2: same page (cursor unchanged). #1 no-ops, #2 retries, #3 processes.
        run2 = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()
        assert run2.already_processed == 1, f"run2: #1 no-ops, got {run2}"
        assert run2.processed == 2, f"run2: #2 and #3 process, got {run2}"
        assert run2.errors == 0
        assert run2.cursor_advanced is True
        assert await load_cursor(session_factory) == "next-mid"

        async with session_factory() as s:
            marker_ids = {
                m.id
                for m in (await s.execute(select(ProcessedEmailEvent))).scalars().all()
            }
        assert "evt-mid-2" in marker_ids, "#2 marker must be written on retry"
        assert "evt-mid-3" in marker_ids, "#3 marker must be written on retry"

    @pytest.mark.asyncio
    async def test_malformed_handled_type_is_page_failure_cursor_unchanged(
        self, session_factory, seeded
    ):
        """Review round 1 repro: a handled-type item (delivered) with a
        malformed payload (missing ``payload.id``) is a PAGE FAILURE —
        cursor untouched, errors=1. Pre-fix the malformed item was logged
        as 'skipped' with errors=0 and the cursor ADVANCED, permanently
        skipping the unprocessed event. Unhandled types (queued/sending/
        sent) remain skippable (covered by
        ``test_queued_sending_sent_skipped_not_processed``)."""
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-malformed",
            contact_email="malformed@acme.com",
            enrollment_id="enr-malformed",
            step_id="estep-malformed",
        )
        # A delivered event (handled type) with payload.id deleted → KeyError
        # inside extract_event_from_api_item.
        bad_item = _api_item(
            event_type="email.delivered",
            event_id="evt-malformed",
            message_id="msg-malformed",
            to_email="malformed@acme.com",
        )
        del bad_item["payload"]["id"]
        page = _page([bad_item], next_cursor="next-malformed")
        await save_cursor(session_factory, POLLER_FEED, "cursor-before-malformed")
        # Key the page by the SEEDED cursor — poll_once resumes from it.
        client = httpx.AsyncClient(
            transport=_transport({"cursor-before-malformed": page})
        )

        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.errors == 1, (
            f"malformed handled-type must be a page error, got {summary}"
        )
        assert summary.skipped == 0, (
            "malformed handled-type must NOT be counted as skipped (pre-fix bug)"
        )
        assert summary.processed == 0
        assert summary.cursor_advanced is False
        assert await load_cursor(session_factory) == "cursor-before-malformed"

    @pytest.mark.asyncio
    async def test_api_error_leaves_cursor_untouched(self, session_factory, seeded):
        """A 401/5xx/network error on the first fetch logs a warning, leaves
        the cursor untouched, and never raises (never crash the host)."""
        await save_cursor(session_factory, POLLER_FEED, "cursor-before-error")

        def handler(request):
            return httpx.Response(401, json={"errors": [{"code": "unauthorized"}]})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        # Must not raise.
        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.errors >= 1
        assert summary.processed == 0
        assert summary.cursor_advanced is False
        assert await load_cursor(session_factory) == "cursor-before-error"

    @pytest.mark.asyncio
    async def test_network_error_leaves_cursor_untouched(self, session_factory):
        """A transport-level network error (connection refused / timeout) is
        caught and never crashes the caller."""
        await save_cursor(session_factory, POLLER_FEED, "cursor-before-neterr")

        def handler(request):
            raise httpx.ConnectError("simulated network failure")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.errors >= 1
        assert await load_cursor(session_factory) == "cursor-before-neterr"

    @pytest.mark.asyncio
    async def test_starts_from_first_page_when_no_cursor(self, session_factory, seeded):
        """On empty/missing cursor, the poller starts from the first page
        (no page[cursor] param). Dedupe makes the backfill safe."""
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-poll-nocursor",
            contact_email="pollnc@acme.com",
            enrollment_id="enr-poll-nocursor",
            step_id="estep-poll-nocursor",
        )

        captured_cursors = []

        def handler(request):
            captured_cursors.append(request.url.params.get("page[cursor]"))
            return httpx.Response(
                200,
                json=_page(
                    [
                        _api_item(
                            event_type="email.delivered",
                            event_id="evt-poll-nocursor",
                            message_id="msg-poll-nocursor",
                            to_email="pollnc@acme.com",
                        )
                    ]
                ),
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert captured_cursors[0] is None, "first fetch must send NO page[cursor]"

    @pytest.mark.asyncio
    async def test_resumes_from_persisted_cursor(self, session_factory, seeded):
        """The poller resumes from the persisted cursor — the first fetch
        sends page[cursor]=<persisted cursor>."""
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-poll-resume",
            contact_email="pollres@acme.com",
            enrollment_id="enr-poll-resume",
            step_id="estep-poll-resume",
        )
        await save_cursor(session_factory, POLLER_FEED, "resume-cursor-123")

        captured_cursors = []

        def handler(request):
            captured_cursors.append(request.url.params.get("page[cursor]"))
            return httpx.Response(
                200,
                json=_page(
                    [
                        _api_item(
                            event_type="email.delivered",
                            event_id="evt-poll-resume",
                            message_id="msg-poll-resume",
                            to_email="pollres@acme.com",
                        )
                    ]
                ),
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert captured_cursors[0] == "resume-cursor-123", (
            "first fetch must resume from the persisted cursor"
        )

    @pytest.mark.asyncio
    async def test_paginates_until_short_page(self, session_factory, seeded):
        """The poller walks full pages until a short page (< page_size), then
        stops. The cursor advances once per full page processed."""
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-poll-page",
            contact_email="pollpage@acme.com",
            enrollment_id="enr-poll-page",
            step_id="estep-poll-page",
        )

        full_item = _api_item(
            event_type="email.delivered",
            event_id="evt-poll-page-1",
            message_id="msg-poll-page",
            to_email="pollpage@acme.com",
        )
        # Page 1: full (page_size=1, 1 item) → next_cursor="c2"
        # Page 2: short (0 items, < page_size=1) → stop
        pages = {
            None: _page([full_item], page_size=1, next_cursor="c2"),
            "c2": _page([], page_size=1, next_cursor=None),
        }
        client = httpx.AsyncClient(transport=_transport(pages))

        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.pages == 2, f"expected 2 pages, got {summary.pages}"
        assert summary.processed == 1
        assert summary.cursor_advanced is True
        # Cursor stays at the last non-null cursor ("c2" from page 1) — the
        # tail page returned no page_cursor, so it must NOT null the persisted
        # cursor (REVOPS-1525 follow-up: pre-fix this was None → full rewalk).
        assert await load_cursor(session_factory) == "c2"


# ── poll_once — unhandled event types are skipped ────────────────────────────


class TestPollOnceSkipsUnhandledTypes:
    @pytest.mark.asyncio
    async def test_queued_sending_sent_skipped_not_processed(
        self, session_factory, seeded
    ):
        """queued/sending/sent events arrive in the events feed but the
        poller must NOT process them (the send path owns send-state). They
        are counted as skipped and do not block the cursor."""
        items = [
            _api_item(event_type="email.queued", event_id="evt-q", message_id="msg-q"),
            _api_item(
                event_type="email.sending", event_id="evt-sd", message_id="msg-sd"
            ),
            _api_item(event_type="email.sent", event_id="evt-st", message_id="msg-st"),
        ]
        page = _page(items)
        client = httpx.AsyncClient(transport=_transport({None: page}))

        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.skipped == 3, f"expected 3 skipped, got {summary}"
        assert summary.processed == 0
        assert summary.errors == 0
        # Tail page (3 items < page_size 25) with no page_cursor → no non-null
        # cursor saved → cursor_advanced False. Skipped events still do NOT
        # block the cursor (they process successfully); the cursor simply has
        # nothing to advance TO on a tail page (REVOPS-1525 follow-up: pre-fix
        # this was True because the poller persisted None unconditionally).
        assert summary.cursor_advanced is False, (
            "skipped (unhandled) events must NOT block the cursor; no "
            "page_cursor on the tail page means nothing to advance to"
        )
        async with session_factory() as s:
            assert (
                len((await s.execute(select(ProcessedEmailEvent))).scalars().all()) == 0
            )


# ── poll_once — tail-page None cursor fix (REVOPS-1525 follow-up) ────────────


class TestPollOnceTailCursorNoneFix:
    """REVOPS-1525 follow-up: the API returns NO ``page_cursor`` on the final
    short page. Pre-fix the poller did ``cursor = next_cursor; await
    save_cursor(cursor)`` unconditionally → it persisted ``None`` on every tail
    page → the next cycle restarted from page 1 and re-walked the entire
    history (dedupe made it correct but unbounded API-load growth as event
    history grows). Fix: only assign + persist when ``next_cursor`` is truthy;
    ``cursor_advanced`` is True only when a non-null cursor was saved.

    Live evidence (2026-08-17): first cycle walked 11 pages and finished
    ``cursor_advanced=True``, but ``email_events_poller_cursor.last_cursor``
    was EMPTY — every subsequent 2-minute cycle restarted from page 1.
    """

    @pytest.mark.asyncio
    async def test_tail_page_with_items_no_page_cursor_keeps_prior_cursor(
        self, session_factory, seeded
    ):
        """Two-page stream: page 1 full with ``next_cursor="c1"``, page 2 short
        WITH items but ``page_cursor`` absent. Pre-fix the poller overwrote
        the cursor with None → next run re-walked from page 1. Post-fix the
        persisted cursor stays at ``"c1"`` (the cursor that pointed AT the tail
        page), so the next run re-fetches ONLY the tail page (small overlap;
        dedupe absorbs it)."""
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-tail-1",
            contact_email="tail1@acme.com",
            enrollment_id="enr-tail-1",
            step_id="estep-tail-1",
        )
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-tail-2",
            contact_email="tail2@acme.com",
            enrollment_id="enr-tail-2",
            step_id="estep-tail-2",
        )
        # Page 1: full (page_size=1, 1 item) → next_cursor="c1"
        # Page 2: short (1 item < page_size=25, items present, NO page_cursor)
        page1 = _page(
            [
                _api_item(
                    event_type="email.delivered",
                    event_id="evt-tail-1",
                    message_id="msg-tail-1",
                    to_email="tail1@acme.com",
                )
            ],
            page_size=1,
            next_cursor="c1",
        )
        page2 = _page(
            [
                _api_item(
                    event_type="email.delivered",
                    event_id="evt-tail-2",
                    message_id="msg-tail-2",
                    to_email="tail2@acme.com",
                )
            ],
            # default page_size=25 → 1 item is short; default next_cursor=None
        )
        pages = {None: page1, "c1": page2}
        client = httpx.AsyncClient(transport=_transport(pages))

        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.processed == 2, f"both events must process, got {summary}"
        assert summary.errors == 0
        assert summary.cursor_advanced is True, (
            "page 1 saved a non-null cursor → advanced"
        )
        # The persisted cursor is "c1" (page 1's next_cursor), NOT None — the
        # tail page's missing page_cursor must NOT overwrite it. Pre-fix this
        # was None → every subsequent cycle re-walked from page 1.
        assert await load_cursor(session_factory) == "c1", (
            "tail page with no page_cursor must NOT null the persisted cursor"
        )

        # Second run: must resume from "c1" (NOT page 1) — re-fetches only the
        # tail page. Markers make the re-pull a no-op.
        captured_cursors = []

        def handler(request):
            cursor = request.url.params.get("page[cursor]")
            captured_cursors.append(cursor)
            if cursor == "c1":
                # Same tail page (re-pulled) — markers absorb the overlap.
                return httpx.Response(200, json=page2)
            return httpx.Response(200, json=_page([]))

        client2 = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        summary2 = await poll_once(session_factory, client2, BASE_URL, API_KEY)
        await client2.aclose()

        assert captured_cursors[0] == "c1", (
            "second run must resume from the persisted tail cursor, not page 1"
        )
        assert summary2.already_processed == 1, (
            f"re-pulled tail event must no-op via marker, got {summary2}"
        )
        assert summary2.processed == 0
        assert summary2.errors == 0
        # Tail page again returns no page_cursor → cursor stays at "c1".
        assert await load_cursor(session_factory) == "c1"

    @pytest.mark.asyncio
    async def test_single_short_first_page_no_cursor_no_prior_stays_none(
        self, session_factory, seeded
    ):
        """Single short first page with items but NO ``page_cursor`` and NO
        prior cursor → nothing to save, persisted cursor stays None, no crash,
        ``cursor_advanced`` False (no non-null cursor was saved). Pre-fix this
        saved None and set cursor_advanced=True."""
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-tail-none",
            contact_email="tailnone@acme.com",
            enrollment_id="enr-tail-none",
            step_id="estep-tail-none",
        )

        # Single short page (1 item < page_size=25), default next_cursor=None.
        page = _page(
            [
                _api_item(
                    event_type="email.delivered",
                    event_id="evt-tail-none",
                    message_id="msg-tail-none",
                    to_email="tailnone@acme.com",
                )
            ]
        )
        client = httpx.AsyncClient(transport=_transport({None: page}))

        summary = await poll_once(session_factory, client, BASE_URL, API_KEY)
        await client.aclose()

        assert summary.processed == 1, f"event must process, got {summary}"
        assert summary.errors == 0
        assert summary.cursor_advanced is False, (
            "no non-null cursor was saved → cursor_advanced must be False"
        )
        # Nothing was saved — persisted cursor stays None (no crash, no row).
        assert await load_cursor(session_factory) is None, (
            "no prior cursor + tail page with no page_cursor → nothing saved"
        )
