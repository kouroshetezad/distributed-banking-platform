#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PY=${PY:-python3}

exec "$PY" -m platform.local_stack "$@"
