#!/usr/bin/env bash
# Usage: compute_diff_between_files.sh <parquet directory 1> <parquet directory 2>
set -euo pipefail
exec "$(dirname "$0")/_submit.sh" diff --left "$1" --right "$2"
