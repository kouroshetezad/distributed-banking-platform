#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PY=python3

echo "==> all groups"
"$PY" -m tests.run_tests "$@"
