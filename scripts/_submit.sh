#!/usr/bin/env bash
# Shared launcher: submits one SparkMain command with the project jar.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$ROOT/target/bal-spark-1.0.0.jar"
# Git Bash on Windows: Spark reads "C:/..." as a URI scheme, so pass a file URI.
if command -v cygpath >/dev/null 2>&1; then
    JAR="file:///$(cygpath -m "$JAR")"
fi
exec spark-submit --class fr.esilv.SparkMain --master "${SPARK_MASTER:-local[*]}" "$JAR" "$@"
