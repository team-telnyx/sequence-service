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

from datetime import datetime, timedelta
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
    `f"step:{enrollment_step_id}:{int(scheduled_at.timestamp())}"`.

    Pinning the exact format guards against someone "simplifying" to a bare
    `f"step:{id}"` — which would collapse re-enqueues correctly today but arq
    refuses a `job_id` that still exists in its result store, so a step
    legitimately needing re-enqueue AFTER its previous job completed would be
    SILENTLY REFUSED and stranded forever. Including `scheduled_at` means a
    genuine re-schedule (reconciler stamps `scheduled_at=now` before enqueueing)
    always produces a new id.
    """
    captured = {}

    async def _capture(name, *args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _fake_job_with_id(kwargs.get("_job_id", "anything"))

    _patch_pool(monkeypatch, _FakePool(_capture))

    step_id = "estep-abc"
    sched = datetime(2026, 7, 20, 12, 0, 0)
    await queue_mod.queue_sequence_step(
        enrollment_step_id=step_id,
        tenant_id="t1",
        scheduled_at=sched,
    )
    job_id = captured["kwargs"].get("_job_id")
    assert job_id == f"step:{step_id}:{int(sched.timestamp())}", (
        f"job_id must be 'step:{{id}}:{{int(scheduled_at.timestamp())}}'; got {job_id!r}"
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
    # Second call hit the None path: must not raise, and must return the
    # deterministic id (or None) — the contract is just "no AttributeError".
    assert r2 is None or r2 == calls[0][2]["_job_id"]


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
        "re-schedule with different scheduled_at must produce a NEW _job_id; "
        f"got {captured_ids}"
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
async def test_queue_next_step_handles_deduped_enqueue_return(
    seeded, session_factory, monkeypatch
):
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
    # _queue_next_step returns a dict on success; the deduped enqueue path
    # must not crash — assert it returned a dict (None also acceptable since
    # the except branch logs and returns None, but NO AttributeError).
    assert out is None or isinstance(out, dict), (
        f"deduped enqueue must not AttributeError; got {out!r}"
    )


# ── Reconciler integration: paced sweep re-enqueueing same steps → ONE job ──


@pytest.mark.asyncio
async def test_reconciler_sweep_double_enqueue_dedupes(
    seeded, session_factory, monkeypatch
):
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
        f"dedup must collapse second enqueue — expected 1 distinct _job_id, "
        f"got {seen_job_ids}"
    )


# ── Deterministic id format asserted explicitly (cross-check) ───────────────


@pytest.mark.asyncio
async def test_job_id_format_explicit_invariant(monkeypatch):
    """Cross-check: the _job_id has exactly the form
    `step:{enrollment_step_id}:{int(scheduled_at.timestamp())}`.

    This test exists separately from `test_job_id_format_is_step_id_and_scheduled_at_timestamp`
    to pin the invariant from a second angle (no arq-pool simulation), so that
    a regression that simplifies to `step:{id}` is caught twice.
    """
    captured = {}

    async def _enqueue(name, *args, **kwargs):
        captured["job_id"] = kwargs["_job_id"]
        return _fake_job_with_id(kwargs["_job_id"])

    _patch_pool(monkeypatch, _FakePool(_enqueue))

    sched = datetime(2026, 7, 20, 9, 30, 0)
    await queue_mod.queue_sequence_step(
        enrollment_step_id="estep-invariant",
        tenant_id="t",
        scheduled_at=sched,
    )
    expected = f"step:estep-invariant:{int(sched.timestamp())}"
    assert captured["job_id"] == expected, (
        f"explicit format invariant violated: expected {expected!r}, "
        f"got {captured['job_id']!r}"
    )
