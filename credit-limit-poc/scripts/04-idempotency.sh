#!/usr/bin/env bash
# Demo step 4: a gateway retry never reserves twice; a reused order ID is refused.
set -e
source "$(dirname "$0")/lib.sh"
tokens

ACCOUNT=ACC-00453
ORDER="$(new_id RETRY)"

bold "Exposure before"
exposure "$ACCOUNT"

bold "First attempt: \$250,000"
check "$ORDER" "$ACCOUNT" 2500 100
bold "Retry, same order ID and payload (the gateway timed out): same answer, nothing new reserved"
check "$ORDER" "$ACCOUNT" 2500 100
bold "Exposure: reserved went up once"
exposure "$ACCOUNT"
pause

bold "Same order ID, different order: 409 Conflict"
check "$ORDER" "$ACCOUNT" 9999 100
