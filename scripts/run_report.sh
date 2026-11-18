#!/usr/bin/env bash
# Usage: run_report.sh
set -euo pipefail
exec "$(dirname "$0")/_submit.sh" report
