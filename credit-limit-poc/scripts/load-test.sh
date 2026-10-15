#!/usr/bin/env bash
# Fixed-rate load test through nginx with a latency report.
#   scripts/load-test.sh [RATE] [SECONDS]      default 2000 checks/s for 60 s
# Orders are small ($10k-$200k) so limits are not exhausted in a few seconds.
# A laptop running every container plus the load generator will usually top out around
# 2,000-3,000 checks/s; the 5,000/s target is a design number, not a laptop number.
source "$(dirname "$0")/lib.sh"

RATE="${1:-2000}"
SECONDS_TO_RUN="${2:-60}"
bold "Load test: $RATE checks/s for $SECONDS_TO_RUN s"
docker compose run --rm gateway-simulator --sim.mode=load --sim.rate="$RATE" \
  --sim.duration="${SECONDS_TO_RUN}s" --sim.min-notional=10000 --sim.max-notional=200000
note "Server-side view: curl -s $API/actuator/prometheus is per-instance; see RUNBOOK for metrics."
