"""REVOPS-1525 — runner-level tests for src/jobs/email_events_poller.py.

Review round 1 found two runner defects:
  1. The completion and catch-all log calls passed structlog-style keyword
     args (``pages=``, ``error=``) to a stdlib logger → ``TypeError`` → exit 1
     on a routine 401 (the 401 is caught inside ``poll_once`` and surfaces as
     a ``PollSummary``; the runner then crashes logging it).
  2. The lock-file-open error branch fell through to ``_run()`` WITHOUT the
     lock, allowing overlapping instances.

These tests prove:
  - A 401-shaped ``PollSummary`` (the routine case) does not crash the runner
    — the completion log line emits cleanly (stdlib %-format args).
  - A ``poll_once`` that raises (the catch-all path) does not crash the runner
    — the catch-all log line emits cleanly.
  - Lock-open failure → exit 0 WITHOUT polling (fail closed).
  - Lock-acquire failure (already held) → exit 0 WITHOUT polling.

``poll_once`` and ``_run`` are mocked so the tests are hermetic — no DB, no
network, no real Telnyx calls. The lock path is exercised for real against a
per-test temp file so the flock single-flight contract is proven, not
stubbed.
"""

from __future__ import annotations

import fcntl
import logging
import os

import pytest

from src.services.email_events_poller import PollSummary


def _isolate_lock(monkeypatch, tmp_path, name="poller.lock"):
    """Point the runner at a per-test temp lock file so concurrent tests or
    a leftover lock from a prior run cannot interfere with flock acquisition."""
    import src.jobs.email_events_poller as runner

    lock_path = str(tmp_path / name)
    monkeypatch.setattr(runner, "LOCK_PATH", lock_path)
    return lock_path


def _stub_env(monkeypatch):
    """Set the env vars _run() reads so it proceeds past the early-return
    guards. DATABASE_URL is a dummy — poll_once is mocked so no connection
    is opened (create_async_engine is lazy)."""
    monkeypatch.setenv("EMAIL_API_KEY", "test-key")
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql+asyncpg://dummy@localhost:5432/dummy"
    )
    monkeypatch.setenv("EMAIL_API_BASE_URL", "https://api.telnyx.com/v2")
    monkeypatch.setenv("EMAIL_API_TIMEOUT", "5.0")


# ── Finding 4a: stdlib logger calls must not TypeError on a routine 401 ────


def test_runner_401_summary_completion_log_does_not_crash(
    monkeypatch, tmp_path, caplog
):
    """Review round 1 repro: a 401 from the Email API is caught inside
    ``poll_once`` and surfaces as a ``PollSummary(errors=1, cursor_advanced=
    False)``. The runner's completion log line must emit cleanly on that
    summary — pre-fix it passed structlog-style kwargs (``pages=``,
    ``processed=``, ...) to a stdlib logger → ``TypeError`` → exit 1."""
    import src.jobs.email_events_poller as runner
    import src.services.email_events_poller as poller_mod

    _isolate_lock(monkeypatch, tmp_path)
    _stub_env(monkeypatch)

    async def fake_poll_once(*args, **kwargs):
        # Simulate poll_once's 401 handling: it caught the PollerAPIError,
        # logged a structlog warning, and returned a summary with errors=1
        # and cursor_advanced=False (cursor untouched).
        return PollSummary(pages=0, processed=0, errors=1, cursor_advanced=False)

    monkeypatch.setattr(poller_mod, "poll_once", fake_poll_once)
    caplog.set_level(logging.INFO, logger="email_events_poller")

    # Must not raise — exit 0.
    runner.main()

    # The completion log line must have been emitted — proves the stdlib
    # %-format call did not TypeError on the summary's fields.
    assert any(
        "Email events poll complete" in r.getMessage() for r in caplog.records
    ), "completion log line must emit (regression: structlog kwargs on stdlib logger)"


def test_runner_run_exception_catchall_log_does_not_crash(
    monkeypatch, tmp_path, caplog
):
    """The runner's catch-all log line (``logger.error("Poller run failed
    ...", error=str(e))``) must emit cleanly when ``_run`` raises — pre-fix
    the ``error=`` structlog kwarg on a stdlib logger → ``TypeError`` → exit
    1 on a failure that escaped ``poll_once`` (e.g. a DB connection error in
    load_cursor)."""
    import src.jobs.email_events_poller as runner
    import src.services.email_events_poller as poller_mod

    _isolate_lock(monkeypatch, tmp_path)
    _stub_env(monkeypatch)

    async def exploding_poll_once(*args, **kwargs):
        raise RuntimeError("simulated failure that escaped poll_once")

    monkeypatch.setattr(poller_mod, "poll_once", exploding_poll_once)
    caplog.set_level(logging.ERROR, logger="email_events_poller")

    # Must not raise — the catch-all swallows to protect the host process.
    runner.main()

    assert any(
        "Poller run failed — swallowing to protect host process" in r.getMessage()
        for r in caplog.records
    ), (
        "catch-all log line must emit (regression: structlog `error=` kwarg on stdlib logger)"
    )


# ── Finding 4b: fail closed on lock-open / lock-acquire failure ────────────


def test_runner_lock_open_failure_exits_without_polling(monkeypatch, tmp_path, caplog):
    """Review round 1: if the lock file cannot be opened, the runner must log
    a warning and exit 0 WITHOUT polling. Pre-fix the branch fell through to
    ``_run()`` without the lock, allowing overlapping instances."""
    import src.jobs.email_events_poller as runner

    # A path in a nonexistent directory makes os.open raise OSError (ENOENT).
    monkeypatch.setattr(runner, "LOCK_PATH", "/nonexistent-dir-1525/poller.lock")
    monkeypatch.setenv("EMAIL_API_KEY", "test-key")  # would poll if not fail-closed
    caplog.set_level(logging.WARNING, logger="email_events_poller")

    poll_called = []

    async def fake_run():
        poll_called.append(True)

    monkeypatch.setattr(runner, "_run", fake_run)

    # Must not raise — exit 0.
    runner.main()

    assert poll_called == [], "must NOT poll when the lock file cannot be opened"
    assert any(
        "Could not open poller lock file" in r.getMessage() for r in caplog.records
    ), "must log a warning explaining the fail-closed exit"


def test_runner_lock_acquire_failure_exits_without_polling(
    monkeypatch, tmp_path, caplog
):
    """If the lock is already held by another fd, the runner must log info
    and exit 0 WITHOUT polling. (This branch was already fail-closed pre-fix
    — the test locks the invariant in.)"""
    import src.jobs.email_events_poller as runner

    lock_path = _isolate_lock(monkeypatch, tmp_path)
    monkeypatch.setenv("EMAIL_API_KEY", "test-key")  # would poll if not fail-closed
    caplog.set_level(logging.INFO, logger="email_events_poller")

    # Hold the lock from a separate fd so the runner's flock(LOCK_NB) fails.
    held_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(held_fd, fcntl.LOCK_EX)

        poll_called = []

        async def fake_run():
            poll_called.append(True)

        monkeypatch.setattr(runner, "_run", fake_run)

        # Must not raise — exit 0.
        runner.main()

        assert poll_called == [], "must NOT poll when the lock is already held"
        assert any(
            "Another email events poller instance is running" in r.getMessage()
            for r in caplog.records
        ), "must log an info line explaining the single-flight exit"
    finally:
        try:
            fcntl.flock(held_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(held_fd)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
