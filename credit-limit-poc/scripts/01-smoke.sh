#!/usr/bin/env bash
# Demo step 1: an approval, an account-level rejection, and a group-level rejection.
set -e
source "$(dirname "$0")/lib.sh"
tokens

bold "Approved: \$542,000 for ACC-00451 (account limit \$39M, group G0401 \$250M)"
check "$(new_id SMOKE)" ACC-00451 10000 54.20
pause

bold "Rejected at ACCOUNT level: \$80M is above any single account limit (\$1M-\$50M)"
check "$(new_id SMOKE)" ACC-00451 800000 100
pause

bold "Group level: ACC-00002 gets a temporary \$500M override, but its manager G0000 is capped at \$100M"
note "PUT /api/v1/limits/ACC-00002/override (credit officer, audited on credit.limits)"
EXPIRES="$(date -u -v+1H +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)"
curl -s -X PUT "$API/api/v1/limits/ACC-00002/override" \
  -H "Authorization: Bearer $OFFICER_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"limitUsd\":500000000,\"expiresAt\":\"$EXPIRES\",\"reason\":\"Demo: block trade approval\"}" | pretty
pause
check "$(new_id SMOKE)" ACC-00002 1500000 100
note "Account headroom is fine with the override; the GROUP limit rejects it."
