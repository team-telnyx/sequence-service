#!/usr/bin/env python3
"""One-time backfill of scheduled_at for pre-fix SCHEDULED steps (audit M4).

Before the M4 fix, sequence_enrollment_steps.scheduled_at was never written, so
every SCHEDULED step has scheduled_at = NULL. The live reconciler deliberately
ignores NULL rows (a NULL is ambiguous — it could be a valid future job, and
re-firing it would send a follow-up early). This script removes the ambiguity by
computing each stuck step's INTENDED fire time:

    scheduled_at = (most-recent SENT step's sent_at in the same enrollment, for an
                    earlier step_number) + this step's delay
                 = enrollment.created_at   (first step, nothing sent yet)

After backfill, the reconciler's normal "overdue" logic does the right thing:
genuinely-overdue follow-ups are recovered; steps whose intended time is still in
the future are left alone.

DRY-RUN by default — prints the bucket breakdown and writes nothing.
  python -m scripts.backfill_scheduled_at_M4                 # dry-run
  python -m scripts.backfill_scheduled_at_M4 --apply         # backfill all
  python -m scripts.backfill_scheduled_at_M4 --apply --skip-older-than-days 7
        # also mark steps whose intended fire is >7 days stale as SKIPPED, so we
        # never blast a follow-up that is hopelessly late. Recommended for the
        # initial recovery so the catch-up burst stays bounded.
"""
import argparse
import sys

import psycopg2

sys.path.insert(0, ".")
from src.config import get_settings  # noqa: E402


# Correlated expression for a step's intended scheduled_at. `es`/`ss`/`enr` are
# the aliases bound in the FROM clause below.
INTENDED = """
COALESCE(
  (SELECT max(es2.sent_at)
     FROM sequence_enrollment_steps es2
     JOIN sequence_steps ss2 ON ss2.id = es2.step_id
    WHERE es2.enrollment_id = es.enrollment_id
      AND es2.status = 'SENT'
      AND ss2.step_number < ss.step_number)
    + make_interval(secs => ss.delay_days * 86400 + ss.delay_hours * 3600),
  enr.created_at
)
"""

FROM_TARGET = """
  FROM sequence_enrollment_steps es
  JOIN sequence_steps ss        ON ss.id  = es.step_id
  JOIN sequence_enrollments enr ON enr.id = es.enrollment_id
 WHERE es.status = 'SCHEDULED' AND es.scheduled_at IS NULL
"""


def _sync_dsn() -> str:
    url = get_settings().database_url
    return url.replace("postgresql+asyncpg://", "postgresql://")


def dry_run(cur) -> None:
    cur.execute(f"""
        WITH t AS (
            SELECT es.id, {INTENDED} AS intended,
                   (SELECT max(es2.sent_at) FROM sequence_enrollment_steps es2
                    JOIN sequence_steps ss2 ON ss2.id=es2.step_id
                    WHERE es2.enrollment_id=es.enrollment_id AND es2.status='SENT'
                      AND ss2.step_number < ss.step_number) AS prior_sent
            {FROM_TARGET}
        )
        SELECT
          count(*)                                                            AS total,
          count(*) FILTER (WHERE prior_sent IS NULL)                          AS first_step_no_prior,
          count(*) FILTER (WHERE intended <  now() - interval '15 min')       AS overdue_recoverable,
          count(*) FILTER (WHERE intended >= now() - interval '15 min')       AS future_leave_alone,
          count(*) FILTER (WHERE intended <  now() - interval '7 days')       AS overdue_gt_7d
        FROM t;
    """)
    total, first_np, overdue, future, gt7 = cur.fetchone()
    print("── M4 scheduled_at backfill — DRY RUN ─────────────────────────")
    print(f"  SCHEDULED steps with NULL scheduled_at : {total}")
    print(f"    • first step, nothing sent yet       : {first_np}")
    print(f"    • overdue (intended <now-15m)         : {overdue}  ← reconciler will recover")
    print(f"        of which >7 days stale            : {gt7}  ← --skip-older-than-days would SKIP")
    print(f"    • future (intended >=now-15m)         : {future}  ← left untouched (protected)")
    print("  Nothing written. Re-run with --apply to backfill.")


def apply_backfill(cur, skip_older_than_days):
    # Pass 1: set scheduled_at = intended fire time for every NULL SCHEDULED step.
    cur.execute(f"""
        UPDATE sequence_enrollment_steps es
           SET scheduled_at = ({INTENDED})
          FROM sequence_steps ss, sequence_enrollments enr
         WHERE ss.id = es.step_id AND enr.id = es.enrollment_id
           AND es.status = 'SCHEDULED' AND es.scheduled_at IS NULL
    """)
    backfilled = cur.rowcount

    skipped = 0
    if skip_older_than_days is not None:
        # Pass 2: steps now scheduled far in the past are hopelessly late — mark
        # SKIPPED so the reconciler never sends them. Only touches what pass 1 set.
        cur.execute("""
            UPDATE sequence_enrollment_steps
               SET status = 'SKIPPED'
             WHERE status = 'SCHEDULED'
               AND scheduled_at < now() - make_interval(days => %s)
        """, (skip_older_than_days,))
        skipped = cur.rowcount

    print(f"── M4 backfill APPLIED ──────────────────────────────")
    print(f"  scheduled_at backfilled : {backfilled}")
    if skip_older_than_days is not None:
        print(f"  marked SKIPPED (>{skip_older_than_days}d stale) : {skipped}")
    print("  Remaining overdue SCHEDULED steps will be recovered by the reconciler.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    ap.add_argument("--skip-older-than-days", type=int, default=None,
                    help="mark steps whose intended fire is older than N days as SKIPPED")
    args = ap.parse_args()

    conn = psycopg2.connect(_sync_dsn())
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            if args.apply:
                apply_backfill(cur, args.skip_older_than_days)
                conn.commit()
            else:
                dry_run(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
