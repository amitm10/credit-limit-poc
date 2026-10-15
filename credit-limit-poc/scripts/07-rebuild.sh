#!/usr/bin/env bash
# Demo step 7: wipe Redis entirely and rebuild it from Kafka. Exposure must match exactly.
# Best run with the flow simulator stopped, so the numbers hold still for the comparison.
set -e
source "$(dirname "$0")/lib.sh"
tokens

snapshot() {
  for a in ACC-00001 ACC-00002 ACC-00451 ACC-00452 ACC-00453 ACC-00455 ACC-CONC; do
    printf '%-10s ' "$a"
    curl -s "$API/api/v1/exposure/$a" -H "Authorization: Bearer $OFFICER_TOKEN" \
      | grep -o '"account":{[^}]*}' | grep -o '"usedUsd":[^,]*,"reservedUsd":[^,]*' || echo "(unavailable)"
  done
}

bold "Exposure before"
BEFORE="$(snapshot)"
echo "$BEFORE"
pause

PRIMARY="$(redis_primary)"
bold "FLUSHALL on $PRIMARY: every limit, reservation and total is gone"
redis_cli FLUSHALL
redis_cli DBSIZE
pause

bold "POST /admin/rebuild: replay credit.limits and credit.reservations"
curl -s -X POST "$PROC/admin/rebuild" | pretty

bold "Exposure after"
AFTER="$(snapshot)"
echo "$AFTER"
if [ "$BEFORE" = "$AFTER" ]; then bold "MATCH: Redis rebuilt exactly from Kafka"; else bold "DIFFERENT (was traffic running?)"; fi
