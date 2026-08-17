"""Deterministic arq job_id for `process_sequence_step` enqueues (REVOPS-1373 sibling).

Live measurement 2026-07-20: 83,286 queued arq jobs against 2,927 live SCHEDULED
steps — ~27 duplicate jobs per step, 79,511 of them firing within 24h. Root
cause: `queue_sequence_step` calls `pool.enqueue_job('process_sequence_step', ...)`
with NO `_job_id`, so arq mints a fresh uuid4 every call. arq deduplicates by
`job_id` only, so with a random one dedup is completely off — every re-enqueue of
the same step (reconciler sweep, circuit_resume, retry paths) mints a brand-new
job for a step that already has one queued.

Fix: give `queue_sequence_step` a deterministic `_job_id` keyed on
`(enrollment_step_id, scheduled_at)` so repeat enqueues for the SAME intended
fire time collapse into one. `scheduled_at` is in the key (not just `step_id`)
so a genuine re-schedule after a prior job completed (reconciler stamps
`scheduled_at=now` before enqueueing) produces a NEW id, while duplicates for
the same fire time collapse — that distinction is the whole point of the fix.

`enqueue_job` returns None when a job with this `_job_id` already exists. The
old `return job.job_id` raises AttributeError the moment dedup starts working,
so callers that capture the return (enrollments.py:306, sequence_step.py:532)
must be None-safe.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

import src.api.enrollments as enr_mod
import src.workers.reconcile as rec
import src.workers.sequence_step as ss  # noqa: F401  (used in _queue_next_step test)
from src.models.models import (
    EnrollmentStatus,
    EnrollmentStepStatus,
    SequenceEnrollment,
    SequenceEnrollmentStep,
)
from src.services import queue as queue_mod


# ── Helpers ──────────────────────────────────────────────────────────────────


class _FakePool:
    """Minimal stand-in for arq's Redis pool — only `enqueue_job` is called.

    The real pool's `enqueue_job` returns Optional[Job]: a Job on first enqueue,
    None when a job with the same `_job_id` already exists (the dedup signal).
    The constructor takes a coroutine that implements that contract.
    """

    def __init__(self, enqueue_fn):
        self._enqueue_fn = enqueue_fn

    async def enqueue_job(self, name, *args, **kwargs):
        return await self._enqueue_fn(name, *args, **kwargs)


async def _enqueue_returns_none(name, *args, **kwargs):
    """Fake pool enqueue that always returns None — simulates a 100% deduped
    state. Used to verify None-safety in callers that capture the return."""
    return None


def _patch_pool(monkeypatch, fake_pool):
    """Replace `queue.get_redis_pool()` with a coroutine returning `fake_pool`.

    `queue_sequence_step` awaits `get_redis_pool()` to fetch the pool, so we
    patch that function (not the module-level `_pool` global) — otherwise the
    lazy `if _pool is None` branch would still try to create a real arq pool.
    """

    async def _fake_get_pool():
        return fake_pool

    monkeypatch.setattr(queue_mod, "get_redis_pool", _fake_get_pool)


def _fake_job_with_id(jid):
    """A minimal stand-in for arq.Job with the only attribute callers use."""
    fake = AsyncMock()
    fake.job_id = jid
    return fake


# ── Deterministic job_id format ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_job_id_format_is_step_id_and_scheduled_at_timestamp(monkeypatch):
    """The `_job_id` passed to `pool.enqueue_job` must be
    `f"step:{enrollment_step_id}:{int(UTC_normalized_scheduled_at.timestamp())}"`.

    Pinning the exact format guards against someone "simplifying" to a bare
    `f"step:{id}"` — which would collapse re-enqueues correctly today but arq
    refuses a `job_id` that still exists in its result store, so a step
    legitimately needing re-enqueue AFTER its previous job completed would be
    SILENTLY REFUSED and stranded forever. Including `scheduled_at` means a
    genuine re-schedule (reconciler stamps `scheduled_at=now` before enqueueing)
    always produces a new id.

    The naive `scheduled_at` is interpreted as UTC (not local) before keying —
    see finding 4. Tests under any `TZ` env value must produce the same key.
    """
    captured = {}

    async def _capture(name, *args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _fake_job_with_id(kwargs.get("_job_id", "anything"))

    _patch_pool(monkeypatch, _FakePool(_capture))

    step_id = "estep-abc"
    sched = datetime(2026, 7, 20, 12, 0, 0)  # naive
    await queue_mod.queue_sequence_step(
        enrollment_step_id=step_id,
        tenant_id="t1",
        scheduled_at=sched,
    )
    job_id = captured["kwargs"].get("_job_id")
    expected = f"step:{step_id}:{int(sched.replace(tzinfo=timezone.utc).timestamp())}"
    assert job_id == expected, (
        f"job_id must be 'step:{{id}}:{{int(UTC-aware scheduled_at.timestamp())}}'; got {job_id!r}"
    )


# ── Dedup: two enqueues for same (step_id, scheduled_at) → ONE job ───────────


@pytest.mark.asyncio
async def test_duplicate_enqueue_for_same_step_and_scheduled_at_dedupes(monkeypatch):
    """Two calls with the same (enrollment_step_id, scheduled_at) produce ONE
    arq job; the second returns the None path without raising.

    arq's `enqueue_job` returns None when a job with the given `_job_id` is
    already queued — the dedup signal. The first call returns a Job with
    `.job_id`; the second returns None. The contract: same `_job_id` in both
    calls, and the function does not raise on None.
    """
    calls = []

    async def _enqueue(name, *args, **kwargs):
        calls.append((name, args, kwargs))
        if len(calls) == 1:
            # First call: returns a real-ish Job
            return _fake_job_with_id(kwargs["_job_id"])
        # Second call: arq returns None when _job_id already queued
        return None

    _patch_pool(monkeypatch, _FakePool(_enqueue))

    sched = datetime(2026, 7, 20, 12, 0, 0)
    await queue_mod.queue_sequence_step(
        enrollment_step_id="estep-1",
        tenant_id="t1",
        scheduled_at=sched,
    )
    r2 = await queue_mod.queue_sequence_step(
        enrollment_step_id="estep-1",
        tenant_id="t1",
        scheduled_at=sched,
    )
    # Same _job_id both calls → dedup
    assert calls[0][2]["_job_id"] == calls[1][2]["_job_id"]
    # Second call hit the None path: must return None outright (the contract
    # the reconciler depends on at reconcile.py:297 — `if job_id is None:
    # continue` — to avoid counting a deduped enqueue as a fresh
    # reconciliation). A regression to returning the id would silently
    # over-count `reconciled` (finding 2).
    assert r2 is None, f"deduped enqueue must return None (reconciler None contract); got {r2!r}"


# ── Re-schedule: same step_id + DIFFERENT scheduled_at → NEW job ────────────


@pytest.mark.asyncio
async def test_reschedule_same_step_with_different_scheduled_at_makes_new_job(
    monkeypatch,
):
    """Same `step_id` with a DIFFERENT `scheduled_at` must produce a NEW distinct
    job — proves the reconciler's rescue path still works after a prior job
    completed (a bare `step:{id}` would be refused by arq forever and the step
    would be stranded).
    """
    captured_ids = []

    async def _enqueue(name, *args, **kwargs):
        captured_ids.append(kwargs["_job_id"])
        return _fake_job_with_id(kwargs["_job_id"])

    _patch_pool(monkeypatch, _FakePool(_enqueue))

    sched1 = datetime(2026, 7, 20, 12, 0, 0)
    sched2 = datetime(2026, 7, 20, 14, 0, 0)  # 2h later — a real re-schedule
    await queue_mod.queue_sequence_step(
        enrollment_step_id="estep-1",
        tenant_id="t1",
        scheduled_at=sched1,
    )
    await queue_mod.queue_sequence_step(
        enrollment_step_id="estep-1",
        tenant_id="t1",
        scheduled_at=sched2,
    )
    assert len(captured_ids) == 2
    assert captured_ids[0] != captured_ids[1], (
        f"re-schedule with different scheduled_at must produce a NEW _job_id; got {captured_ids}"
    )


# ── Return-value safety: None path exercised through callers that capture ───


@pytest.mark.asyncio
async def test_create_enrollment_handles_deduped_enqueue_return(
    seeded, session_factory, monkeypatch
):
    """enrollments.create_enrollment captures `job_id = await queue_sequence_step(...)`
    at line ~306. With dedup working, `queue_sequence_step` can return None on
    a duplicate first-step enqueue. The caller must not AttributeError.

    The conftest client fixture patches queue_sequence_step to a constant mock;
    here we use a real queue_sequence_step with a None-returning pool and
    confirm the enroll path doesn't raise. The simplest realistic path is to
    drive the real function through monkeypatch and assert on the response.
    """
    _patch_pool(monkeypatch, _FakePool(_enqueue_returns_none))
    # Re-wire the enrollments module's queue_sequence_step reference so the
    # deduped None return flows through the actual function (not conftest's
    # constant mock).
    monkeypatch.setattr(
        enr_mod,
        "queue_sequence_step",
        queue_mod.queue_sequence_step,
    )
    from httpx import ASGITransport, AsyncClient
    from src.api.main import app
    from src.models.base import get_db

    async def _override_get_db():
        async with session_factory() as s:
            yield s

    monkeypatch.setattr("src.api.main.async_session", session_factory)
    # Auth is env-var-based now — patch settings so the middleware accepts the test key.
    import src.api.main as main_mod

    monkeypatch.setattr(main_mod.settings, "sequence_service_api_key", seeded["api_key"])
    app.dependency_overrides[get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(
                "/api/enrollments/",
                headers={"X-API-Key": seeded["api_key"]},
                json={
                    "sequence_id": seeded["sequence_id"],
                    "contact_email": "dedup-test@acme.com",
                },
            )
        # 201 created — the deduped (None) enqueue must not raise AttributeError
        # up through the enroll path. If it did, we'd get 500.
        assert resp.status_code == 201, (
            f"deduped first-step enqueue must not break create_enrollment; "
            f"got {resp.status_code} body={resp.text}"
        )
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_queue_next_step_handles_deduped_enqueue_return(seeded, session_factory, monkeypatch):
    """sequence_step._queue_next_step captures `job_id = await queue_sequence_step(...)`
    at line ~532. With dedup working, that can return None. The function must
    not AttributeError on None.
    """
    monkeypatch.setattr(ss.settings, "send_jitter_enabled", False, raising=False)
    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id="enr-x",
                sequence_id=seeded["sequence_id"],
                mailbox_id=seeded["active_mailbox_id"],
                contact_email="qnx@acme.com",
                contact_name="QX",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=1,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="es-next",
                enrollment_id="enr-x",
                step_id="step-2",
                mailbox_id=seeded["active_mailbox_id"],
                status=EnrollmentStepStatus.PENDING,
                scheduled_at=None,
            )
        )
        await s.commit()

    _patch_pool(monkeypatch, _FakePool(_enqueue_returns_none))

    async with session_factory() as db:
        enr = await db.get(SequenceEnrollment, "enr-x")
        # Must not raise AttributeError on the None return path.
        out = await ss._queue_next_step(
            db=db,
            enrollment=enr,
            current_step_number=1,
            tenant_id=seeded["tenant_id"],
        )
    # _queue_next_step returns a dict on success with keys
    # `enrollment_step_id`, `step_number`, `delay_seconds`, `job_id`. The
    # deduped enqueue path (job_id=None) must not crash and must still return
    # the dict shape — `_queue_next_step` only returns None in its except branch
    # (logged failure), so a dedup is NOT a None path here. Pin the shape so a
    # regression to None (which would silently drop the next-step bookkeeping)
    # is caught (finding 2).
    assert isinstance(out, dict), (
        f"deduped enqueue must not AttributeError and must return the dict shape; got {out!r}"
    )
    assert out.get("enrollment_step_id") == "es-next", (
        f"next-step dict must reference the next enrollment step; got {out!r}"
    )
    assert out.get("job_id") is None, (
        f"deduped enqueue must surface job_id=None to the caller; got {out!r}"
    )


# ── Reconciler integration: paced sweep re-enqueueing same steps → ONE job ──


@pytest.mark.asyncio
async def test_reconciler_sweep_double_enqueue_dedupes(seeded, session_factory, monkeypatch):
    """Two consecutive reconciler sweeps re-enqueueing the SAME step with the
    SAME `scheduled_at` must produce ONE job, not two — proving the dedup
    mechanism collapses the live flood (reconciler + circuit_resume + retry
    paths all enqueueing the same step).

    The reconciler pushes `scheduled_at=now` after a successful enqueue so the
    second sweep would normally see a fresh scheduled_at. To isolate the dedup
    invariant, we reset `scheduled_at` back to `past` between sweeps so both
    see the same (step_id, scheduled_at) → same `_job_id` → arq returns None on
    the second enqueue. The reconciler must treat that as "already in flight"
    and NOT count it as a new reconciliation.
    """
    # Make pacing non-binding so the step is reconciled.
    monkeypatch.setattr(rec.settings, "reconcile_pacing_window_hours", 1, raising=False)
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)

    past = datetime.utcnow() - timedelta(hours=2)
    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id="enr-dbl",
                sequence_id=seeded["sequence_id"],
                mailbox_id=seeded["active_mailbox_id"],
                contact_email="dbl@acme.com",
                contact_name="Dbl",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=1,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="estep-dbl",
                enrollment_id="enr-dbl",
                step_id="step-1",
                mailbox_id=None,
                status=EnrollmentStepStatus.SCHEDULED,
                scheduled_at=past,
                custom_subject="Hi",
                custom_body="<p>B</p>",
            )
        )
        await s.commit()

    # Real queue_sequence_step with a dedup-aware fake pool: same _job_id → None
    seen_job_ids = []

    async def _enqueue(name, *args, **kwargs):
        jid = kwargs["_job_id"]
        if jid in seen_job_ids:
            return None  # arq's dedup signal
        seen_job_ids.append(jid)
        return _fake_job_with_id(jid)

    _patch_pool(monkeypatch, _FakePool(_enqueue))

    cms = [
        patch.object(rec, "async_session", session_factory),
        patch.object(rec, "queue_sequence_step", queue_mod.queue_sequence_step),
    ]
    for c in cms:
        c.start()
    try:
        out1 = await rec.reconcile_scheduled_steps({})
        # Reset scheduled_at back to past to simulate the same fire-time being
        # re-observed (i.e. the second enqueue is for the SAME intended fire
        # time, which is what dedup collapses).
        async with session_factory() as s:
            step = await s.get(SequenceEnrollmentStep, "estep-dbl")
            step.scheduled_at = past
            step.status = EnrollmentStepStatus.SCHEDULED
            await s.commit()
        out2 = await rec.reconcile_scheduled_steps({})
    finally:
        for c in cms:
            c.stop()
    # The dedup mechanism must collapse the second enqueue into None (no new
    # arq job). Since queue_sequence_step didn't raise, we got here. The
    # observable proof: only ONE _job_id was passed to the fake pool — the
    # second call returned None (arq dedup) and the reconciler's "reconciled"
    # count reflects that the second enqueue was deduped (count stays at 1,
    # not 2, because the reconciler increments only on a successful enqueue).
    assert out1["reconciled"] == 1, f"first sweep should reconcile 1; got {out1}"
    assert out2["reconciled"] == 0, (
        f"second sweep on same (step, scheduled_at) should be deduped → 0 new "
        f"jobs; got {out2}. seen_job_ids={seen_job_ids}"
    )
    # Only ONE distinct _job_id passed to the fake pool.
    assert len(seen_job_ids) == 1, (
        f"dedup must collapse second enqueue — expected 1 distinct _job_id, got {seen_job_ids}"
    )


# ── Deterministic id format asserted explicitly (cross-check) ───────────────


@pytest.mark.asyncio
async def test_job_id_format_explicit_invariant(monkeypatch):
    """Cross-check: the _job_id has exactly the form
    `step:{enrollment_step_id}:{int(UTC_normalized_scheduled_at.timestamp())}`.

    This test exists separately from `test_job_id_format_is_step_id_and_scheduled_at_timestamp`
    to pin the invariant from a second angle (no arq-pool simulation), so that
    a regression that simplifies to `step:{id}` is caught twice. The naive
    `scheduled_at` is interpreted as UTC before keying (finding 4).
    """
    captured = {}

    async def _enqueue(name, *args, **kwargs):
        captured["job_id"] = kwargs["_job_id"]
        return _fake_job_with_id(kwargs["_job_id"])

    _patch_pool(monkeypatch, _FakePool(_enqueue))

    sched = datetime(2026, 7, 20, 9, 30, 0)  # naive
    await queue_mod.queue_sequence_step(
        enrollment_step_id="estep-invariant",
        tenant_id="t",
        scheduled_at=sched,
    )
    expected = f"step:estep-invariant:{int(sched.replace(tzinfo=timezone.utc).timestamp())}"
    assert captured["job_id"] == expected, (
        f"explicit format invariant violated: expected {expected!r}, got {captured['job_id']!r}"
    )


# ── Finding 1 (PRIMARY): capacity-defer must produce a STABLE fire time ───────
# Live 2026-07-20: 78,436 of 83,286 queued arq jobs sit in a single hour bucket
# at the 00:05 UTC capacity reset — ~27 dupes/step. The relative fire-time
# `datetime.utcnow() + timedelta(seconds=seconds_until_capacity_reset())` lets
# the result land anywhere in the final second before the reset depending on
# `utcnow()`'s microsecond fraction, giving TWO possible
# `int(scheduled_at.timestamp())` values per step per reset instead of one — so
# the dedup key still varies and arq does not collapse. The fix uses the
# ABSOLUTE `next_capacity_reset()` (already exists in mailbox_rotation.py),
# which returns the reset instant with second=0, microsecond=0 — identical for
# every call in the day. This guard fails on the relative implementation.


@pytest.mark.asyncio
async def test_capacity_defer_produces_identical_job_id_across_microsecond_skew(
    seeded, session_factory, monkeypatch
):
    """Two capacity-defer enqueues for the SAME step made at different microsecond
    offsets within the same reset window must produce the SAME `_job_id`.

    Drives the real `process_sequence_step` through the at-capacity path with
    `utcnow()` patched to two different microsecond fractions straddling a whole-
    second boundary (0.0 and 0.5). With the relative implementation:
      call 1: utcnow=12:00:00.0, delta=3900.0, int=3900, sched=13:05:00.0
      call 2: utcnow=12:00:00.5, delta=3899.5, int=3899, sched=13:04:59.5
    → two different `int(scheduled_at.timestamp())` → two different _job_ids.
    With the absolute fix both calls produce `next_capacity_reset()` (a single
    instant with second=0, microsecond=0) → one _job_id.
    """
    # Real queue_sequence_step with a capturing fake pool.
    captured_sched = []

    async def _enqueue(name, *args, **kwargs):
        captured_sched.append(kwargs["_job_id"])
        return _fake_job_with_id(kwargs["_job_id"])

    _patch_pool(monkeypatch, _FakePool(_enqueue))

    # Make an enrollment whose sticky mailbox is at the hard cap so the worker
    # takes the capacity-defer path.
    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id="enr-cap-dedup",
                sequence_id=seeded["sequence_id"],
                mailbox_id=seeded["full_mailbox_id"],  # sent_today == daily_send_limit
                contact_email="capdedup@acme.com",
                contact_name="CD",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=0,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="estep-cap-dedup",
                enrollment_id="enr-cap-dedup",
                step_id="step-1",
                mailbox_id=seeded["full_mailbox_id"],
                status=EnrollmentStepStatus.PENDING,
                scheduled_at=None,
                custom_subject="Hi",
                custom_body="<p>Body</p>",
            )
        )
        await s.commit()

    # Patch async_session and queue_sequence_step on the worker module so the
    # real queue_sequence_step (with our capturing pool) is exercised.
    from src.services import queue as queue_mod
    from src.services import mailbox_rotation as mb

    # Two utcnow values that straddle a whole-second boundary relative to the
    # next reset (12:00:00.0 and 12:00:00.5 vs reset at 13:05:00.0). The relative
    # impl computes `defer_delay = seconds_until_capacity_reset()` (truncated
    # whole seconds) THEN `scheduled = utcnow + defer_delay`. With a fixed
    # reset instant, the two calls produce:
    #   call 1: utcnow=12:00:00.0, delta=3900.0 → int=3900 → sched=13:05:00.0
    #   call 2: utcnow=12:00:00.5, delta=3899.5 → int=3899 → sched=13:04:59.5
    # → two different `int(scheduled_at.timestamp())` → two _job_ids.
    # The absolute fix uses `next_capacity_reset()` directly so both calls
    # produce 13:05:00.0 → one _job_id.
    fixed_targets = [
        datetime(2026, 7, 20, 12, 0, 0, 0),  # microsecond=0
        datetime(2026, 7, 20, 12, 0, 0, 500000),  # microsecond=0.5
    ]
    reset_target = datetime(2026, 7, 20, 13, 5, 0, 0)  # 00:05 UTC the next day

    call_index = {"i": 0}

    class _PatchedDatetime(datetime):
        @classmethod
        def utcnow(cls):
            idx = call_index["i"]
            call_index["i"] = idx + 1
            return fixed_targets[idx]

    # `seconds_until_capacity_reset` uses its own datetime.now(timezone.utc)
    # (real clock), so patch it to return the truncated delta consistent with
    # our patched utcnow — otherwise both calls get the same defer_delay and
    # never straddle. NB: the worker calls `seconds_until_capacity_reset()`
    # BEFORE `datetime.utcnow()` (line 248 then 260), so we peek the next
    # fixed_target (not yet consumed) to compute the delta.
    def _fake_seconds_until_reset(_now=None):
        idx = min(call_index["i"], len(fixed_targets) - 1)
        delta = (reset_target - fixed_targets[idx]).total_seconds()
        return max(1, int(delta))

    monkeypatch.setattr(mb, "seconds_until_capacity_reset", _fake_seconds_until_reset)
    monkeypatch.setattr(ss, "seconds_until_capacity_reset", _fake_seconds_until_reset)
    monkeypatch.setattr(ss, "datetime", _PatchedDatetime)

    with (
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "queue_sequence_step", queue_mod.queue_sequence_step),
    ):
        await ss.process_sequence_step({}, "estep-cap-dedup", seeded["tenant_id"])
        await ss.process_sequence_step({}, "estep-cap-dedup", seeded["tenant_id"])

    # The two enqueues must produce the SAME _job_id. With the relative impl
    # they differ because `int(scheduled_at.timestamp())` straddles the second
    # boundary (13:05:00.0 → epoch X; 13:04:59.5 → epoch X-1).
    assert len(captured_sched) == 2, (
        f"expected 2 enqueues, got {len(captured_sched)}: {captured_sched}"
    )
    assert captured_sched[0] == captured_sched[1], (
        "capacity-defer for the SAME step at different microsecond offsets must "
        "produce the SAME _job_id (absolute reset instant) — relative "
        "implementation lets utcnow()'s microsecond fraction shift "
        "int(scheduled_at.timestamp()) across a second boundary. "
        f"got {captured_sched}"
    )


# ── Test (b): stored scheduled_at is naive after a capacity defer ────────────
# The `sequence_enrollment_steps.scheduled_at` column is
# `timestamp WITHOUT time zone` holding naive UTC. `next_capacity_reset()`
# returns a tz-aware datetime (uses `datetime.now(timezone.utc)`); assigning
# it aware would error or silently shift the value. The capacity branch MUST
# `.replace(tzinfo=None)` before storing.


@pytest.mark.asyncio
async def test_capacity_defer_stores_naive_scheduled_at(seeded, session_factory, monkeypatch):
    """After a capacity-defer, the step's `scheduled_at` must be naive
    (tzinfo is None) — guards against the aware-datetime-into-naive-column
    trap this repo has been bitten by three times today.
    """
    captured_sched = []

    async def _enqueue(name, *args, **kwargs):
        captured_sched.append(kwargs.get("_job_id"))
        return _fake_job_with_id(kwargs["_job_id"])

    _patch_pool(monkeypatch, _FakePool(_enqueue))

    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id="enr-naive",
                sequence_id=seeded["sequence_id"],
                mailbox_id=seeded["full_mailbox_id"],
                contact_email="naive@acme.com",
                contact_name="N",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=0,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="estep-naive",
                enrollment_id="enr-naive",
                step_id="step-1",
                mailbox_id=seeded["full_mailbox_id"],
                status=EnrollmentStepStatus.PENDING,
                scheduled_at=None,
                custom_subject="Hi",
                custom_body="<p>Body</p>",
            )
        )
        await s.commit()

    from src.services import queue as queue_mod

    with (
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "queue_sequence_step", queue_mod.queue_sequence_step),
    ):
        await ss.process_sequence_step({}, "estep-naive", seeded["tenant_id"])

    async with session_factory() as s:
        step = await s.get(SequenceEnrollmentStep, "estep-naive")
        assert step.scheduled_at is not None, "defer must set scheduled_at"
        assert step.scheduled_at.tzinfo is None, (
            f"stored scheduled_at must be naive (tzinfo is None) — the column "
            f"is `timestamp WITHOUT time zone`. got tzinfo={step.scheduled_at.tzinfo!r}, "
            f"value={step.scheduled_at!r}"
        )


# ── Test (c): dedup key is identical under two different TZ env values ──────
# `.timestamp()` on a naive datetime interprets it as LOCAL time, so the key
# would change with ambient TZ. The fix normalizes to UTC before keying. Use
# `os.environ['TZ']` + `time.tzset()` to prove the key is TZ-invariant.


@pytest.mark.asyncio
async def test_dedup_key_is_tz_invariant(monkeypatch):
    """The same naive `scheduled_at` produces the SAME `_job_id` under two
    different `TZ` env values. Without the UTC normalization the key would
    vary with ambient TZ (and across a DST transition would silently re-enqueue
    duplicates instead of deduping).
    """
    import os
    import time

    captured = []

    async def _capture(name, *args, **kwargs):
        captured.append(kwargs["_job_id"])
        return _fake_job_with_id(kwargs["_job_id"])

    _patch_pool(monkeypatch, _FakePool(_capture))

    sched = datetime(2026, 7, 20, 17, 30, 0)  # naive
    expected = f"step:estep-tz:{int(sched.replace(tzinfo=timezone.utc).timestamp())}"

    saved_tz = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "America/Chicago"
        time.tzset()
        await queue_mod.queue_sequence_step(
            enrollment_step_id="estep-tz",
            tenant_id="t1",
            scheduled_at=sched,
        )
        key_chicago = captured[-1]

        os.environ["TZ"] = "UTC"
        time.tzset()
        await queue_mod.queue_sequence_step(
            enrollment_step_id="estep-tz",
            tenant_id="t1",
            scheduled_at=sched,
        )
        key_utc = captured[-1]
    finally:
        if saved_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = saved_tz
        time.tzset()

    assert key_chicago == expected, (
        f"key under America/Chicago must match UTC-normalized expected; "
        f"got {key_chicago!r} expected {expected!r}"
    )
    assert key_utc == expected, (
        f"key under UTC must match UTC-normalized expected; got {key_utc!r} expected {expected!r}"
    )
    assert key_chicago == key_utc, (
        "dedup key must be TZ-invariant — same naive scheduled_at under "
        "different TZ env values must produce the same _job_id. "
        f"chicago={key_chicago!r} utc={key_utc!r}"
    )


# ── Test (d): reconciler enqueue failure leaves scheduled_at unchanged ──────
# Pre-PR behavior advanced `scheduled_at` only on success. The current code
# advances BEFORE the enqueue try-block, so an enqueue exception commits the
# advance at the end of the loop. The fix reverts `scheduled_at` and
# decrements `mailbox_advanced` on the exception path, so the step IS
# re-selectable on the next sweep (no 900s grace-window stranding).


@pytest.mark.asyncio
async def test_reconciler_enqueue_failure_leaves_scheduled_at_unchanged(
    seeded, session_factory, monkeypatch
):
    """An enqueue exception in the reconciler must revert the step's
    `scheduled_at` to its original past-due value and decrement
    `mailbox_advanced`, so the step IS re-selected on the next sweep.
    """
    monkeypatch.setattr(rec.settings, "reconcile_pacing_window_hours", 1, raising=False)
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)

    past = datetime.utcnow() - timedelta(hours=2)
    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id="enr-fail",
                sequence_id=seeded["sequence_id"],
                mailbox_id=seeded["active_mailbox_id"],
                contact_email="fail@acme.com",
                contact_name="F",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=1,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="estep-fail",
                enrollment_id="enr-fail",
                step_id="step-1",
                mailbox_id=None,
                status=EnrollmentStepStatus.SCHEDULED,
                scheduled_at=past,
                custom_subject="Hi",
                custom_body="<p>B</p>",
            )
        )
        await s.commit()

    # Real queue_sequence_step that ALWAYS raises (simulates redis down).
    async def _raising_enqueue(*args, **kwargs):
        raise RuntimeError("redis down — simulating enqueue failure")

    cms = [
        patch.object(rec, "async_session", session_factory),
        patch.object(rec, "queue_sequence_step", _raising_enqueue),
    ]
    for c in cms:
        c.start()
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        for c in cms:
            c.stop()

    # The reconciler must not count the failed step as reconciled, and must
    # report advanced=0 (the revert cancels the would-be advance).
    assert out["reconciled"] == 0, (
        f"failed enqueue must not count as reconciled; got {out['reconciled']}"
    )
    assert out.get("advanced") == 0, (
        f"failed enqueue must revert the advance (advanced=0); got {out.get('advanced')}"
    )

    # The step's scheduled_at must be UNCHANGED (still past-due, re-selectable
    # on the next sweep).
    async with session_factory() as s:
        step = await s.get(SequenceEnrollmentStep, "estep-fail")
        assert step.scheduled_at == past, (
            f"failed enqueue must leave scheduled_at unchanged (re-selectable); "
            f"expected {past!r}, got {step.scheduled_at!r}"
        )
        assert step.status == EnrollmentStepStatus.SCHEDULED, (
            f"failed enqueue must leave the step SCHEDULED; got {step.status}"
        )


# ── Test (e): `advanced` is present in the returned dict ─────────────────────
# Finding 5: `advanced` was accumulated and gated the commit but was absent
# from both the log line and the returned dict. An operator cannot tell
# "reconciled 0 because everything deduped (healthy)" from "reconciled 0
# because nothing was eligible" without it.


@pytest.mark.asyncio
async def test_reconciler_return_dict_contains_advanced(seeded, session_factory, monkeypatch):
    """The reconciler's returned dict must contain `advanced` alongside
    `reconciled`, `skipped_at_capacity`, etc.
    """
    monkeypatch.setattr(rec.settings, "reconcile_pacing_window_hours", 1, raising=False)
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)

    past = datetime.utcnow() - timedelta(hours=2)
    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id="enr-adv",
                sequence_id=seeded["sequence_id"],
                mailbox_id=seeded["active_mailbox_id"],
                contact_email="adv@acme.com",
                contact_name="A",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=1,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="estep-adv",
                enrollment_id="enr-adv",
                step_id="step-1",
                mailbox_id=None,
                status=EnrollmentStepStatus.SCHEDULED,
                scheduled_at=past,
                custom_subject="Hi",
                custom_body="<p>B</p>",
            )
        )
        await s.commit()

    # Use a normal enqueue that returns a fake job id (newly enqueued).
    async def _ok_enqueue(*args, **kwargs):
        return _fake_job_with_id("job-adv-1")

    cms = [
        patch.object(rec, "async_session", session_factory),
        patch.object(rec, "queue_sequence_step", _ok_enqueue),
    ]
    for c in cms:
        c.start()
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        for c in cms:
            c.stop()

    assert "advanced" in out, (
        f"returned dict must contain 'advanced' (finding 5); keys={list(out.keys())}"
    )
    assert out["advanced"] == 1, f"one step reconciled → advanced must be 1; got {out['advanced']}"
    assert out["reconciled"] == 1, (
        f"one step reconciled → reconciled must be 1; got {out['reconciled']}"
    )
    # Also check per_mailbox carries advanced for the active mailbox.
    pm = out.get("per_mailbox", {})
    assert seeded["active_mailbox_id"] in pm, (
        f"per_mailbox must include the active mailbox; got {list(pm.keys())}"
    )
    assert "advanced" in pm[seeded["active_mailbox_id"]], (
        f"per_mailbox entry must contain 'advanced'; keys={list(pm[seeded['active_mailbox_id']].keys())}"
    )
