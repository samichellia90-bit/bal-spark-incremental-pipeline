#!/usr/bin/env bash
# Usage: run_daily_file_integration.sh <YYYY-MM-DD> <csv file or directory>
set -euo pipefail
exec "$(dirname "$0")/_submit.sh" daily --date "$1" --input "$2"
