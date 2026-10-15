#!/usr/bin/env bash
# Demo step 3: one order through its lifecycle. Watch reserved move to used, then the rest released.
set -e
source "$(dirname "$0")/lib.sh"
tokens

ACCOUNT=ACC-00452
GROUP=G0402
ORDER="$(new_id LIFE)"

bold "Exposure before"
exposure "$ACCOUNT"
pause

bold "1. Check: 10,000 @ \$100 = \$1,000,000 reserved"
check "$ORDER" "$ACCOUNT" 10000 100
pause

bold "2. Venue ACK (Kafka): order is WORKING, no longer an orphan candidate"
order_event "$ORDER" "$ACCOUNT" "$GROUP" ACK 1
sleep 1
reservation "$ORDER" "$ACCOUNT"
pause

bold "3. Partial FILL: cumulative 4,000 @ \$100 -> \$400,000 moves from reserved to used"
order_event "$ORDER" "$ACCOUNT" "$GROUP" FILL 2 4000 100
sleep 1
reservation "$ORDER" "$ACCOUNT"
pause

bold "4. Same FILL delivered twice: cumulative amounts make it a no-op"
order_event "$ORDER" "$ACCOUNT" "$GROUP" FILL 2 4000 100
sleep 1
reservation "$ORDER" "$ACCOUNT"
pause

bold "5. CANCEL the rest: remaining \$600,000 released"
order_event "$ORDER" "$ACCOUNT" "$GROUP" CANCEL 3
sleep 1
reservation "$ORDER" "$ACCOUNT"

bold "Exposure after: used +\$400,000, reserved back where it was"
exposure "$ACCOUNT"
