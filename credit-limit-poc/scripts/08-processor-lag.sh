#!/usr/bin/env bash
# Demo step 8: the exposure processor stops. Checks keep working, and headroom stays
# conservative: a fill that has not been applied yet is still counted as reserved.
set -e
source "$(dirname "$0")/lib.sh"
tokens

ACCOUNT=ACC-00455
GROUP=G0405
ORDER="$(new_id LAG)"

bold "Stopping exposure-processor"
docker compose stop exposure-processor

bold "Check still works: \$500,000 reserved"
check "$ORDER" "$ACCOUNT" 5000 100
order_event "$ORDER" "$ACCOUNT" "$GROUP" ACK 1
order_event "$ORDER" "$ACCOUNT" "$GROUP" CANCEL 2
note "The venue cancelled the order, but nobody has applied the event yet."
bold "Exposure: the \$500,000 is still reserved (conservative: can cause a false reject, never an over-commit)"
exposure "$ACCOUNT"
pause

bold "Starting exposure-processor: it resumes from its committed offset and catches up"
docker compose start exposure-processor
for _ in $(seq 1 30); do
  sleep 2
  state="$(curl -s "$API/api/v1/reservations/$ORDER?accountId=$ACCOUNT" -H "Authorization: Bearer $GATEWAY_TOKEN" | json_field state)"
  [ "$state" = "CANCELLED" ] && break
done
reservation "$ORDER" "$ACCOUNT"
exposure "$ACCOUNT"
