#!/usr/bin/env bash
# End-to-end check on the three sample snapshots of examples/.
# Ingests three days, rebuilds the first day from the diffs and compares it
# with a direct ingestion of the first day.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$ROOT/target/smoke"
LOG="$WORK/smoke.log"
rm -rf "$WORK"
mkdir -p "$WORK/main" "$WORK/reference"

run() { "$ROOT/scripts/$1" "${@:2}" 2>>"$WORK/spark.err" | tee -a "$LOG" | grep -E '^\[(DAILY|RECOMPUTE|DIFF)\]' || true; }
expect() { grep -qF "$1" "$LOG" || { echo "FAIL: expected '$1'"; exit 1; }; }

export BAL_STORAGE_ROOT="$WORK/main"
run run_daily_file_integration.sh 2025-01-02 "$ROOT/examples/day1.csv"
run run_daily_file_integration.sh 2025-01-03 "$ROOT/examples/day2.csv"
run run_daily_file_integration.sh 2025-01-04 "$ROOT/examples/day3.csv"
run recompute_and_extract_dump_at_date.sh 2025-01-02 "$WORK/dump_day1"

export BAL_STORAGE_ROOT="$WORK/reference"
run run_daily_file_integration.sh 2025-01-02 "$ROOT/examples/day1.csv"
run compute_diff_between_files.sh "$WORK/dump_day1" "$WORK/reference/bal_latest"

expect "day=2025-01-02 prev=0 new=4 inserted=4 updated=0 deleted=0"
expect "day=2025-01-03 prev=4 new=4 inserted=1 updated=1 deleted=1"
expect "day=2025-01-04 prev=4 new=4 inserted=0 updated=1 deleted=0"
expect "reverse-applying 2 day(s)"
expect "[DIFF] inserted=0 updated=0 deleted=0"
echo "SMOKE TEST OK"
