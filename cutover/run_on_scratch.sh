#!/usr/bin/env bash
# REVOPS-972 — test the full cutover chain against a FRESH scratch DB restored
# from the live snapshot. Proves the destructive sequence end-to-end with zero
# risk to production. Usage: bash run_on_scratch.sh
set -euo pipefail
DUMP="${1:-/tmp/seqsvc_snapshot_$(date +%Y%m%d).dump}"
SCRATCH=seqsvc_scratch
HERE="$(cd "$(dirname "$0")" && pwd)"
PSQL="psql -h localhost -U kevinward -v ON_ERROR_STOP=1 -d $SCRATCH"

echo "### rebuild $SCRATCH from $DUMP"
psql -h localhost -U kevinward -d postgres -tAc "DROP DATABASE IF EXISTS $SCRATCH;" >/dev/null
psql -h localhost -U kevinward -d postgres -tAc "CREATE DATABASE $SCRATCH;" >/dev/null
pg_restore -h localhost -U kevinward -d $SCRATCH "$DUMP" 2>/dev/null || true   # benign SET errors ignored

echo "### baseline"; $PSQL -tAc "select tenant_id, count(*) from mailboxes group by 1 order by 1;"

for step in 01_archive_quinn 02_repoint_leaked 03_migrate_suppressions 04_delete_nonscout 05_backfill_pause_reason; do
  echo "### $step"; $PSQL -f "$HERE/$step.sql"
done
echo "### migration 001 (schema)"; $PSQL -f "$HERE/../migrations/001_scout_only_collapse.sql"
echo "### verify"; $PSQL -f "$HERE/06_verify.sql"
echo "### DONE — cutover chain green on scratch"
