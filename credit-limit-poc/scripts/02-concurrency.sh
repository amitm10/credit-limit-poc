#!/usr/bin/env bash
# Demo step 2: 500 parallel checks against one $10M account, total demand 2x the headroom.
# Pass = approved exposure never exceeds the headroom and equals what Redis reserved.
# The approved orders are cancelled afterwards, so the test can be run again a few seconds later.
source "$(dirname "$0")/lib.sh"

bold "500 parallel checks against ACC-CONC (\$10M limit), each for 2x headroom / 500"
docker compose run --rm gateway-simulator --sim.mode=concurrency --sim.requests="${1:-500}"
