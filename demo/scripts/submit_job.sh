#!/usr/bin/env bash
# Submit the pipeline to a running Ray cluster via the Jobs API.
# Usage: RAY_ADDRESS=http://head:8265 scripts/submit_job.sh 2026-09-05
set -euo pipefail
RUN_DATE="${1:?run date required}"
cd "$(dirname "$0")/.."
uv export --no-dev --no-hashes --format requirements-txt > /tmp/fraudflow-reqs.txt
ray job submit \
  --working-dir . \
  --runtime-env-json "{\"pip\": \"/tmp/fraudflow-reqs.txt\", \"env_vars\": {\"PYTHONPATH\": \"src\"}}" \
  -- python -m fraudflow.cli run --config configs/cluster.yaml --run-date "$RUN_DATE"
