"""Gmail I/O blocks the arq event loop (root cause of 2026-07-20 timeouts).

Every Gmail call in src/ is a SYNCHRONOUS blocking HTTPS request issued from
an async worker coroutine, freezing the whole asyncio event loop:

- signal_detection.py: detect_signals() calls gmail.get_replies_to_threads()
  which loops get_thread() once per thread sequentially. At ~1,300 threads
  (75 sends/day × 21-day window) that is ~260s of frozen loop per mailbox.
- sequence_step.py: process_sequence_step() calls gmail.send_html_email().

The frozen loop means max_jobs=10 is fiction (jobs run serially), and
job_timeout=300 cannot fire while the loop is blocked, so durations blow
past 300s: live logs show `1139.20s ! detect_signals failed, TimeoutError`
and `2156.30s ! cron:reconcile_scheduled_steps max retries 1 exceeded`.

This test file pins the regression guards for three fixes:

1. Offload the blocking calls via asyncio.to_thread so the event loop stays
   responsive while the Gmail HTTP request runs in a worker thread. The
   event-loop responsiveness test FAILS on main and PASSES after the fix —
   it is the before/after evidence the planner gate requires.

2. Per-mailbox threading.Lock around the offloaded calls. GmailService.get_inbox
   returns a class-level cached singleton (cls._instances[email]); the
   underlying googleapiclient service wraps httplib2 which is NOT thread-safe.
   Two concurrent jobs on the SAME mailbox must not share an unguarded
   instance.

3. Incremental scan: skip threads already recorded as a Signal so a second
   run over unchanged data issues materially fewer get_thread() fetches than
   the first (1,300 → ~0 for threads whose reply was already recorded). The
   21-day lookback window stays intact — we bound the WORK, not the window.
"""

import asyncio
import threading
import time
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import pytest

from src.models.models import (
    SequenceEnrollment,
    SequenceEnrollmentStep,
    SentEmail,
    Signal,
    SignalType,
    EnrollmentStatus,
    EnrollmentStepStatus,
)
from src.services.gmail import GmailService
import src.workers.signal_detection as sd
import src.workers.sequence_step as ss


# ---------------------------------------------------------------------------
# Helpers — realistic fixtures, not lazy empty mocks.
# ---------------------------------------------------------------------------


async def _async_false(*a, **k):
    return False


def _stub_gmail_with_sleep(seconds: float, sentinel_payload=None):
    """Build a GmailService whose get_replies_to_threads BLOCKS for `seconds`.

    Mirrors the live blocking call: it sleeps the CALLING thread. On current
    main the call runs inline in the event loop's thread, so it freezes the
    loop; after the fix the call runs in a worker thread, so the loop keeps
    making progress.
    """
    g = GmailService.__new__(GmailService)  # bypass __init__/auth
    g.inbox = "quinn.c@telnyx.com"
    g._service = MagicMock()
    g._credentials = None
    g._lock = threading.Lock()  # mirror __init__ (which the bypass skips)

    def _blocking_get_replies(thread_ids):
        time.sleep(seconds)  # blocks the calling thread
        return sentinel_payload or []

    g.get_replies_to_threads = _blocking_get_replies
    return g


async def _seed_sent_emails(session_factory, mailbox_id, n_threads):
    """Seed n_threads SentEmail rows in the last 21 days, each with a thread_id."""
    now = datetime.utcnow()
    async with session_factory() as s:
        # Minimal enrollment + step + sent emails so detect_signals' query
        # (selectinload(SentEmail.enrollment_step).selectinload(...enrollment))
        # has something to join on.
        enr = SequenceEnrollment(
            id="enr-sd",
            sequence_id="seq-1",
            mailbox_id=mailbox_id,
            contact_email="vp@acme.com",
            contact_name="VP",
            timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE,
            current_step=1,
        )
        s.add(enr)
        step = SequenceEnrollmentStep(
            id="estep-sd",
            enrollment_id="enr-sd",
            step_id="step-1",
            mailbox_id=mailbox_id,
            status=EnrollmentStepStatus.SENT,
            custom_subject="Hi",
            custom_body="<p>Body</p>",
        )
        s.add(step)
        for i in range(n_threads):
            s.add(
                SentEmail(
                    id=f"sent-{i}",
                    message_id=f"mid-{i}",
                    thread_id=f"tid-{i}",
                    mailbox_id=mailbox_id,
                    enrollment_step_id="estep-sd",
                    subject="Hi",
                    body="Body",
                    to_email="vp@acme.com",
                    from_email="quinn.c@telnyx.com",
                    sent_at=now - timedelta(days=1),
                )
            )
        await s.commit()
    return "estep-sd"


def _sd_patches(session_factory, gmail_instance):
    """Patch detect_signals' external deps so it is drivable in isolation."""
    return [
        patch.object(sd, "async_session", session_factory),
        patch.object(sd.GmailService, "get_inbox", return_value=gmail_instance),
    ]


def _enable_gmail(monkeypatch):
    monkeypatch.setattr(sd.settings, "gmail_enabled", True, raising=False)
    monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)


# ===========================================================================
# Fix 1A — event loop stays responsive while get_replies_to_threads blocks.
# ===========================================================================


@pytest.mark.asyncio
async def test_detect_signals_does_not_block_event_loop(
    seeded, session_factory, monkeypatch
):
    """Given a blocking get_replies_to_threads, a concurrently-scheduled
    coroutine must still make progress while detect_signals is running.

    Fails on current main (sync call freezes the loop), passes after the fix
    (asyncio.to_thread offloads the block to a worker thread).
    """
    _enable_gmail(monkeypatch)
    await _seed_sent_emails(session_factory, seeded["active_mailbox_id"], n_threads=3)
    gmail = _stub_gmail_with_sleep(seconds=2.0)

    # A second coroutine that flips a flag every ~50ms once it gets control.
    tick = {"n": 0}

    async def _heartbeat():
        for _ in range(20):
            await asyncio.sleep(0.05)
            tick["n"] += 1

    cms = _sd_patches(session_factory, gmail)
    for c in cms:
        c.start()
    try:
        hb = asyncio.create_task(_heartbeat())
        await sd.detect_signals({}, seeded["active_mailbox_id"], seeded["tenant_id"])
        # Snapshot BEFORE awaiting the heartbeat tail — this is what proves
        # whether the heartbeat ran DURING the blocking call.
        ticks_during_call = tick["n"]
        # Let any in-flight tick drain so the task completes cleanly.
        await asyncio.wait_for(hb, timeout=2.0)
    finally:
        for c in cms:
            c.stop()

    # If the loop was blocked for 2.0s the heartbeat could not have started,
    # so ticks_during_call is 0. If the call was offloaded the heartbeat ran
    # concurrently (~20 ticks over ~1s), so ticks_during_call is well above 5.
    assert ticks_during_call >= 5, (
        f"event loop was blocked: heartbeat advanced {ticks_during_call} ticks "
        f"during the 2s blocking call (need >=5 to prove offloading to a worker thread)"
    )


# ===========================================================================
# Fix 1B — event loop stays responsive while send_html_email blocks.
# ===========================================================================


@pytest.mark.asyncio
async def test_process_sequence_step_send_does_not_block_event_loop(
    seeded, session_factory, monkeypatch
):
    """Given a blocking send_html_email, a concurrently-scheduled coroutine
    must still make progress while process_sequence_step is sending.

    Fails on current main, passes after the fix.
    """
    # Build a real enrollment step that process_sequence_step will run.
    async with session_factory() as s:
        enr = SequenceEnrollment(
            id="enr-send-block",
            sequence_id=seeded["sequence_id"],
            mailbox_id=seeded["active_mailbox_id"],
            contact_email="vp@acme.com",
            contact_name="VP",
            timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE,
            current_step=0,
        )
        s.add(enr)
        es = SequenceEnrollmentStep(
            id="estep-send-block",
            enrollment_id="enr-send-block",
            step_id="step-1",
            mailbox_id=seeded["active_mailbox_id"],
            status=EnrollmentStepStatus.PENDING,
            custom_subject="Hi",
            custom_body="<p>Body</p>",
        )
        s.add(es)
        await s.commit()
    step_id = "estep-send-block"

    # Gmail inbox whose send_html_email BLOCKS the calling thread for 2s.
    def _blocking_send(*a, **k):
        time.sleep(2.0)
        return {"message_id": "m-block", "thread_id": "t-block"}

    inbox = MagicMock()
    inbox.send_html_email = _blocking_send

    monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
    cms = [
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "check_suppressed", new=_async_false),
        patch.object(ss, "check_circuit_breaker", new=_async_false),
        patch.object(ss, "check_send_window", new=lambda tz: None),
        patch.object(ss.GmailService, "get_inbox", return_value=inbox),
    ]
    for c in cms:
        c.start()
    try:
        tick = {"n": 0}

        async def _heartbeat():
            for _ in range(20):
                await asyncio.sleep(0.05)
                tick["n"] += 1

        hb = asyncio.create_task(_heartbeat())
        await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
        ticks_during_send = tick["n"]
        await asyncio.wait_for(hb, timeout=2.0)
    finally:
        for c in cms:
            c.stop()

    assert ticks_during_send >= 5, (
        f"event loop was blocked during send: heartbeat advanced "
        f"{ticks_during_send} ticks during the 2s blocking send "
        f"(need >=5 to prove offloading to a worker thread)"
    )


# ===========================================================================
# Fix 2 — per-mailbox thread safety: concurrent same-mailbox access is
# serialized via a lock on the cached GmailService instance.
# ===========================================================================


@pytest.mark.asyncio
async def test_concurrent_same_mailbox_runs_are_serialized(
    seeded, session_factory, monkeypatch
):
    """Two detect_signals runs on the SAME mailbox launched concurrently must
    not interleave their get_replies_to_threads calls — the per-mailbox lock
    must serialize them so they cannot corrupt the shared httplib2 state.

    Proves the lock exists by asserting the blocking regions do NOT overlap.
    """
    _enable_gmail(monkeypatch)
    await _seed_sent_emails(session_factory, seeded["active_mailbox_id"], n_threads=3)

    # Make get_replies_to_threads record its enter/exit wallclock times.
    timeline = []  # list of ("enter"|"exit", ts)

    def _tracked_get_replies(thread_ids):
        timeline.append(("enter", time.monotonic()))
        time.sleep(0.3)
        timeline.append(("exit", time.monotonic()))
        return []

    gmail = GmailService.__new__(GmailService)
    gmail.inbox = "quinn.c@telnyx.com"
    gmail._service = MagicMock()
    gmail._credentials = None
    gmail._lock = threading.Lock()
    gmail.get_replies_to_threads = _tracked_get_replies

    cms = _sd_patches(session_factory, gmail)
    for c in cms:
        c.start()
    try:
        # Patch create_signal_webhook so detect_signals does not need a tenant webhook.
        with patch.object(sd, "create_signal_webhook", new=_async_false):
            r1, r2 = await asyncio.gather(
                sd.detect_signals({}, seeded["active_mailbox_id"], seeded["tenant_id"]),
                sd.detect_signals({}, seeded["active_mailbox_id"], seeded["tenant_id"]),
            )
    finally:
        for c in cms:
            c.stop()

    # Reconstruct regions from the timeline.
    enters = [t for tag, t in timeline if tag == "enter"]
    exits = [t for tag, t in timeline if tag == "exit"]
    assert len(enters) == 2 and len(exits) == 2, (
        "both runs must have entered get_replies"
    )

    # Two non-overlapping regions: one's exit is <= the other's enter.
    regions = sorted(zip(enters, exits))
    region1_exit, region2_enter = regions[0][1], regions[1][0]
    assert region1_exit <= region2_enter, (
        "get_replies regions overlapped — the two concurrent same-mailbox runs "
        "executed the blocking call in parallel without the per-mailbox lock"
    )


# ===========================================================================
# Fix 3 — incremental scan: 2nd run over unchanged data fetches fewer threads.
# ===========================================================================


@pytest.mark.asyncio
async def test_second_run_fetches_fewer_threads(seeded, session_factory, monkeypatch):
    """After the first run records replies as Signals, a second run over the
    SAME threads must issue materially fewer get_thread() fetches — it skips
    threads whose reply was already recorded.

    First run: N threads fetched.
    Second run: 0 threads fetched (all already recorded).
    """
    _enable_gmail(monkeypatch)
    n_threads = 5
    await _seed_sent_emails(session_factory, seeded["active_mailbox_id"], n_threads)

    # A realistic get_replies_to_threads that tracks call count AND emits a
    # reply for each thread so the first run records one Signal per thread.
    call_log = {"calls": 0, "threads_fetched": []}

    def _get_replies(thread_ids):
        call_log["calls"] += 1
        replies = []
        for tid in thread_ids:
            call_log["threads_fetched"].append(tid)
            replies.append(
                {
                    "message_id": f"reply-{tid}",
                    "thread_id": tid,
                    "from": "vp@acme.com",
                    "subject": "Re: Hi",
                    "date": "Mon, 20 Jul 2026 12:00:00 +0000",
                    "snippet": "yes please",
                    "label_ids": ["INBOX"],
                    "is_bounce": False,
                    "is_ooo": False,
                }
            )
        return replies

    gmail = GmailService.__new__(GmailService)
    gmail.inbox = "quinn.c@telnyx.com"
    gmail._service = MagicMock()
    gmail._credentials = None
    gmail._lock = threading.Lock()
    gmail.get_replies_to_threads = _get_replies

    cms = _sd_patches(session_factory, gmail)
    for c in cms:
        c.start()
    try:
        with patch.object(sd, "create_signal_webhook", new=_async_false):
            r1 = await sd.detect_signals(
                {}, seeded["active_mailbox_id"], seeded["tenant_id"]
            )
            first_threads_fetched = len(call_log["threads_fetched"])
            # Snapshot the call log length so the delta isolates run 2's fetches.
            fetch_count_before_run2 = len(call_log["threads_fetched"])
            r2 = await sd.detect_signals(
                {}, seeded["active_mailbox_id"], seeded["tenant_id"]
            )
            second_threads_fetched = (
                len(call_log["threads_fetched"]) - fetch_count_before_run2
            )
    finally:
        for c in cms:
            c.stop()

    # First run: all n_threads fetched, all recorded as Signals.
    assert r1["signals_detected"] == n_threads
    assert first_threads_fetched == n_threads

    # Second run: zero NEW fetches — every thread already has a recorded Signal.
    # (No new Signals means we never reached the signal-create path, which
    # only happens if the threads were skipped before the fetch.)
    assert second_threads_fetched == 0, (
        f"second run fetched {second_threads_fetched} threads but should have "
        f"skipped all {n_threads} already-recorded threads"
    )
    assert r2["signals_detected"] == 0
    assert r2.get("threads_skipped_incremental") == n_threads


# ===========================================================================
# Send semantics unchanged — the send still goes through with identical args.
# ===========================================================================


@pytest.mark.asyncio
async def test_send_passes_through_identical_args(seeded, session_factory, monkeypatch):
    """After offloading send_html_email to a thread, the call still receives
    the identical keyword args (subject/body/thread_id/message_id/bcc/etc).
    """
    async with session_factory() as s:
        enr = SequenceEnrollment(
            id="enr-args",
            sequence_id=seeded["sequence_id"],
            mailbox_id=seeded["active_mailbox_id"],
            contact_email="vp@acme.com",
            contact_name="VP",
            timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE,
            current_step=0,
        )
        s.add(enr)
        es = SequenceEnrollmentStep(
            id="estep-args",
            enrollment_id="enr-args",
            step_id="step-1",
            mailbox_id=seeded["active_mailbox_id"],
            status=EnrollmentStepStatus.PENDING,
            custom_subject="Hi Args",
            custom_body="<p>Args Body</p>",
        )
        s.add(es)
        await s.commit()
    step_id = "estep-args"

    inbox = MagicMock()
    inbox.send_html_email = MagicMock(
        return_value={"message_id": "m-args", "thread_id": "t-args"}
    )

    monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
    cms = [
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "check_suppressed", new=_async_false),
        patch.object(ss, "check_circuit_breaker", new=_async_false),
        patch.object(ss, "check_send_window", new=lambda tz: None),
        patch.object(ss.GmailService, "get_inbox", return_value=inbox),
    ]
    for c in cms:
        c.start()
    try:
        await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
    finally:
        for c in cms:
            c.stop()

    assert inbox.send_html_email.call_count == 1
    kwargs = inbox.send_html_email.call_args.kwargs
    # The non-configurable pass-throughs — the key thing the live send path
    # depends on for SFDC BCC + threading + RFC 8058.
    assert kwargs["to"] == "vp@acme.com"
    assert kwargs["subject"] == "Hi Args"
    assert kwargs["html_body"]  # non-empty HTML body
    assert kwargs["plain_text_fallback"]  # non-empty plain fallback
    assert "bcc" in kwargs  # may be the SFDC address or None
    assert "list_unsubscribe" in kwargs
    assert "one_click" in kwargs
    assert "sender_name" in kwargs
