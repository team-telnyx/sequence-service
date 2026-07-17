#!/usr/bin/env python3
"""One-time backlog remediation: cull stale enrollments + re-baseline the rest.

REVOPS-1376 (epic REVOPS-1373). Two passes in one transaction:

  1. CULL — ACTIVE enrollments older than 21 days → PAUSE (pause_reason='manual')
     + SKIP their remaining PENDING/SCHEDULED steps. These are hopelessly
     behind; sending a touch weeks/months after the last one is not a real
     cadence. `pause_reason='manual'` is NOT auto-resumed by circuit_resume
     (which only resumes `pause_reason='circuit_breaker'`), so the cull is
     permanent unless an operator explicitly force-resumes via the API. Do NOT
     use COMPLETED (conflates with naturally-finished).

  2. RE-BASELINE — ACTIVE enrollments ≤21 days: for each SCHEDULED step, set
     scheduled_at = enrollment.created_at + step offset (the absolute-from-
     enrollment cadence, REVOPS-1375). If that target is past-due, schedule
     at now + min_step_gap_hours (the catch-up min-spacing guard, REVOPS-1376)
     so the remaining touches don't fire back-to-back. PENDING steps are left
     alone — the fixed worker schedules them correctly on advance.

Idempotent:
  - Cull: second run finds 0 ACTIVE >21d with remaining PENDING/SCHEDULED
    (they're PAUSED). True no-op.
  - Re-baseline: only touches SCHEDULED steps where scheduled_at IS NULL OR
    abs(scheduled_at - (created_at + offset)) > 1 hour (not already baselined).
    On-cadence steps re-compute the same value (no-op). Past-due steps get
    re-scheduled to now + min_gap; they're about to fire anyway, so a second
    run nudging them forward by another gap is harmless.

DRY-RUN by default — prints baseline + planned changes and writes nothing.
  python -m scripts.remediate_backlog_REVOPS_1376          # dry-run
  python -m scripts.remediate_backlog_REVOPS_1376 --apply  # remediate

Reconcile the dry-run BASELINE buckets against
~/scout-admission-cadence/.planning/BASELINE.md §6 before authorizing --apply.
"""

import argparse
import sys

import psycopg2

sys.path.insert(0, ".")
from src.config import get_settings  # noqa: E402


CULL_AGE_DAYS = 21
# Only re-baseline SCHEDULED steps whose scheduled_at is not already within
# 1h of the absolute-from-enrollment offset (idempotency guard).
REBASELINE_TOLERANCE_SECONDS = 3600

# Absolute-from-enrollment intended scheduled_at (REVOPS-1375). `es`/`ss`/`enr`
# are the aliases bound in the FROM clauses below.
INTENDED = """
enr.created_at
  + make_interval(secs => ss.delay_days * 86400 + ss.delay_hours * 3600)
"""


def _sync_dsn() -> str:
    url = get_settings().database_url
    return url.replace("postgresql+asyncpg://", "postgresql://")


def _baseline(cur):
    """Return (n_gt21, m_gt21, n_le21, p_pending, s_scheduled) for the 21-day cut.

    n_gt21: ACTIVE enrollments older than 21 days.
    m_gt21: remaining PENDING/SCHEDULED steps on those enrollments.
    n_le21: ACTIVE enrollments 21 days old or newer.
    p_pending: PENDING steps on ≤21d ACTIVE enrollments.
    s_scheduled: SCHEDULED steps on ≤21d ACTIVE enrollments.
    """
    cur.execute(
        f"""
        WITH gt21 AS (
            SELECT id FROM sequence_enrollments
            WHERE status = 'ACTIVE'
              AND created_at < now() - make_interval(days => {CULL_AGE_DAYS})
        ), le21 AS (
            SELECT id FROM sequence_enrollments
            WHERE status = 'ACTIVE'
              AND created_at >= now() - make_interval(days => {CULL_AGE_DAYS})
        )
        SELECT
            (SELECT count(*) FROM gt21)                                                     AS n_gt21,
            (SELECT count(*) FROM sequence_enrollment_steps es
               JOIN gt21 ON gt21.id = es.enrollment_id
              WHERE es.status IN ('PENDING', 'SCHEDULED'))                                  AS m_gt21,
            (SELECT count(*) FROM le21)                                                     AS n_le21,
            (SELECT count(*) FROM sequence_enrollment_steps es
               JOIN le21 ON le21.id = es.enrollment_id
              WHERE es.status = 'PENDING')                                                 AS p_pending,
            (SELECT count(*) FROM sequence_enrollment_steps es
               JOIN le21 ON le21.id = es.enrollment_id
              WHERE es.status = 'SCHEDULED')                                              AS s_scheduled
        """
    )
    return cur.fetchone()


def _cull_counts(cur):
    """Enrollments to pause + steps to skip (>21d ACTIVE). Same as baseline gt21."""
    cur.execute(
        f"""
        WITH gt21 AS (
            SELECT id FROM sequence_enrollments
            WHERE status = 'ACTIVE'
              AND created_at < now() - make_interval(days => {CULL_AGE_DAYS})
        )
        SELECT
            (SELECT count(*) FROM gt21)                                                     AS enrollments,
            (SELECT count(*) FROM sequence_enrollment_steps es
               JOIN gt21 ON gt21.id = es.enrollment_id
              WHERE es.status IN ('PENDING', 'SCHEDULED'))                                  AS steps
        """
    )
    return cur.fetchone()


def _rebaseline_counts(cur):
    """SCHEDULED steps to re-schedule (≤21d ACTIVE, not already baselined),
    split by past-due vs on-cadence."""
    min_gap_hours = get_settings().min_step_gap_hours
    cur.execute(
        f"""
        WITH candidates AS (
            SELECT es.id, {INTENDED} AS intended
              FROM sequence_enrollment_steps es
              JOIN sequence_steps ss        ON ss.id  = es.step_id
              JOIN sequence_enrollments enr ON enr.id = es.enrollment_id
             WHERE enr.status = 'ACTIVE'
               AND enr.created_at >= now() - make_interval(days => {CULL_AGE_DAYS})
                AND es.status = 'SCHEDULED'
                AND (
                    es.scheduled_at IS NULL
                    OR (({INTENDED}) >= now() AND abs(extract(epoch from (es.scheduled_at - ({INTENDED})))) > {REBASELINE_TOLERANCE_SECONDS})
                    OR (({INTENDED}) <  now() AND es.scheduled_at < now())
                )
        )
        SELECT
            count(*)                                                          AS total,
            count(*) FILTER (WHERE intended <  now())                       AS past_due,
            count(*) FILTER (WHERE intended >= now())                       AS on_cadence
        FROM candidates
        """,
    )
    total, past_due, on_cadence = cur.fetchone()
    return total, past_due, on_cadence, min_gap_hours


def _print_header(apply):
    mode = "APPLY" if apply else "DRY RUN"
    print(f"── REVOPS-1376 backlog remediation — {mode} ──────────────────")


def _print_baseline(n_gt21, m_gt21, n_le21, p_pending, s_scheduled):
    print("BASELINE (21-day cut):")
    print(
        f"  ACTIVE enrollments >21d  : {n_gt21} ({m_gt21} remaining PENDING/SCHEDULED steps)"
    )
    print(
        f"  ACTIVE enrollments ≤21d  : {n_le21} ({p_pending} PENDING, {s_scheduled} SCHEDULED steps)"
    )


def _print_plan(
    cull_enr, cull_steps, reb_total, reb_past, reb_oncadence, min_gap_hours
):
    print()
    print("CULL (>21d ACTIVE → PAUSE/SKIP):")
    print(f"  enrollments to pause      : {cull_enr}")
    print(f"  steps to skip             : {cull_steps}")
    print()
    print("RE-BASELINE (≤21d ACTIVE SCHEDULED):")
    print(f"  steps to re-schedule      : {reb_total}")
    print(f"    of which past-due → now+gap : {reb_past}")
    print(f"    of which on-cadence        : {reb_oncadence}")


def dry_run(cur):
    n_gt21, m_gt21, n_le21, p_pending, s_scheduled = _baseline(cur)
    cull_enr, cull_steps = _cull_counts(cur)
    reb_total, reb_past, reb_oncadence, min_gap_hours = _rebaseline_counts(cur)

    _print_header(apply=False)
    _print_baseline(n_gt21, m_gt21, n_le21, p_pending, s_scheduled)
    _print_plan(cull_enr, cull_steps, reb_total, reb_past, reb_oncadence, min_gap_hours)
    print("Nothing written. Re-run with --apply to remediate.")


def apply(cur):
    n_gt21, m_gt21, n_le21, p_pending, s_scheduled = _baseline(cur)
    cull_enr_before, cull_steps_before = _cull_counts(cur)
    reb_total, reb_past, reb_oncadence, min_gap_hours = _rebaseline_counts(cur)

    _print_header(apply=True)
    _print_baseline(n_gt21, m_gt21, n_le21, p_pending, s_scheduled)
    _print_plan(
        cull_enr_before,
        cull_steps_before,
        reb_total,
        reb_past,
        reb_oncadence,
        min_gap_hours,
    )

    # Pass 1 — CULL: SKIP remaining steps, PAUSE enrollments (>21d ACTIVE).
    cur.execute(
        f"""
        UPDATE sequence_enrollment_steps es
            SET status = 'SKIPPED', updated_at = now()
          FROM sequence_enrollments enr
         WHERE enr.id = es.enrollment_id
           AND enr.status = 'ACTIVE'
           AND enr.created_at < now() - make_interval(days => {CULL_AGE_DAYS})
           AND es.status IN ('PENDING', 'SCHEDULED')
        """
    )
    steps_skipped = cur.rowcount

    cur.execute(
        f"""
        UPDATE sequence_enrollments
            SET status = 'PAUSED', pause_reason = 'manual', updated_at = now()
         WHERE status = 'ACTIVE'
           AND created_at < now() - make_interval(days => {CULL_AGE_DAYS})
        """
    )
    enrollments_paused = cur.rowcount

    # Pass 2 — RE-BASELINE: SCHEDULED steps of ≤21d ACTIVE enrollments.
    # Only touch steps not already baselined (idempotency guard).
    # Past-due → now + min_gap; on-cadence → created_at + offset.
    cur.execute(
        f"""
        UPDATE sequence_enrollment_steps es
            SET scheduled_at = CASE
                WHEN ({INTENDED}) < now()
                THEN now() + make_interval(hours => %s) + (random() * interval '1 hour')
                ELSE ({INTENDED})
            END, updated_at = now()
           FROM sequence_steps ss, sequence_enrollments enr
          WHERE ss.id = es.step_id AND enr.id = es.enrollment_id
            AND enr.status = 'ACTIVE'
            AND enr.created_at >= now() - make_interval(days => {CULL_AGE_DAYS})
            AND es.status = 'SCHEDULED'
            AND (
                es.scheduled_at IS NULL
                OR (({INTENDED}) >= now() AND abs(extract(epoch from (es.scheduled_at - ({INTENDED})))) > {REBASELINE_TOLERANCE_SECONDS})
                OR (({INTENDED}) <  now() AND es.scheduled_at < now())
            )
        """,
        (min_gap_hours,),
    )
    rebaselined = cur.rowcount

    print()
    print("── APPLIED ───────────────────────────────────────")
    print(f"  enrollments paused       : {enrollments_paused}")
    print(f"  steps skipped            : {steps_skipped}")
    print(f"  steps re-baselined       : {rebaselined}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--apply", action="store_true", help="write changes (default: dry-run)"
    )
    args = ap.parse_args()

    conn = psycopg2.connect(_sync_dsn())
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        dbname = cur.fetchone()[0]
        if dbname != "sequence_service":
            conn.close()
            sys.exit(
                f"ABORT: connected to '{dbname}', expected 'sequence_service'. "
                f"Set DATABASE_URL=postgresql+asyncpg://<user>@localhost:5432/sequence_service "
                f"(the repo .env points at the scout DB)."
            )
        print(f"Target DB: {dbname}")
    try:
        with conn.cursor() as cur:
            if args.apply:
                apply(cur)
                conn.commit()
            else:
                dry_run(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
