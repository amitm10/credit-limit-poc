#!/usr/bin/env bash
# Demo step 5: kill one of the three API instances under live traffic.
# Run scripts/flow.sh start first and watch scripts/flow.sh logs in another terminal.
source "$(dirname "$0")/lib.sh"

bold "Killing credit-api-2 (SIGKILL, no graceful shutdown)"
docker compose kill credit-api-2
note "nginx retries the in-flight request on another instance (safe: checks are idempotent by order ID)."
note "Watch the flow logs: 503/errors should stay at or near zero."
pause

bold "Bringing credit-api-2 back"
docker compose start credit-api-2
note "It rejoins behind nginx once it answers; nothing to rebalance because it holds no state."
