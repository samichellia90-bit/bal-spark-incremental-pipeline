#!/usr/bin/env bash
# Usage: recompute_and_extract_dump_at_date.sh <YYYY-MM-DD> <output directory>
set -euo pipefail
exec "$(dirname "$0")/_submit.sh" recompute --date "$1" --output "$2"
