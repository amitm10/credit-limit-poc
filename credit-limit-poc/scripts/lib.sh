#!/usr/bin/env bash
# Shared helpers for the demo scripts. Works with the bash 3.2 that ships with macOS.

API="${API:-http://localhost:8080}"
PROC="${PROC:-http://localhost:8081}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
note() { printf '\033[2m%s\033[0m\n' "$*"; }
pause() { if [ -z "${NO_PAUSE:-}" ]; then printf '\033[2m[press enter]\033[0m'; read -r _; fi; }

pretty() {
  if command -v jq >/dev/null 2>&1; then jq .
  elif command -v python3 >/dev/null 2>&1; then python3 -m json.tool
  else cat; echo
  fi
}

# Extracts a top-level or nested string/number field from JSON without jq: json_field decision
json_field() {
  grep -o "\"$1\":[^,}]*" | head -1 | sed -e "s/\"$1\"://" -e 's/"//g'
}

token() { # token GATEWAY|CREDIT_OFFICER
  curl -fs -X POST "$API/dev/token?role=$1&subject=demo-$1" | json_field token
}

GATEWAY_TOKEN=""
OFFICER_TOKEN=""
tokens() {
  GATEWAY_TOKEN="$(token GATEWAY)"
  OFFICER_TOKEN="$(token CREDIT_OFFICER)"
  if [ -z "$GATEWAY_TOKEN" ]; then
    echo "Cannot get a token from $API. Is the stack up? (scripts/00-up.sh)" >&2
    exit 1
  fi
}

LAST_JSON=""
check() { # check ORDER_ID ACCOUNT QUANTITY PRICE  (sets LAST_JSON)
  local body="{\"orderId\":\"$1\",\"accountId\":\"$2\",\"side\":\"BUY\",\"quantity\":$3,\"price\":$4}"
  note "POST /api/v1/credit-checks $body"
  local out status
  out="$(curl -s -w $'\n%{http_code} %{time_total}' -X POST "$API/api/v1/credit-checks" \
    -H "Authorization: Bearer $GATEWAY_TOKEN" -H 'Content-Type: application/json' -d "$body")"
  status="${out##*$'\n'}"
  LAST_JSON="${out%$'\n'*}"
  echo "$LAST_JSON" | pretty
  note "HTTP ${status% *} in ${status#* }s"
}

exposure() { # exposure ACCOUNT
  note "GET /api/v1/exposure/$1"
  curl -s "$API/api/v1/exposure/$1" -H "Authorization: Bearer $OFFICER_TOKEN" | pretty
}

reservation() { # reservation ORDER_ID ACCOUNT
  curl -s "$API/api/v1/reservations/$1?accountId=$2" -H "Authorization: Bearer $GATEWAY_TOKEN" | pretty
}

order_event() { # order_event ORDER_ID ACCOUNT GROUP TYPE SEQ [CUM_QTY] [PRICE]
  local json now
  now="$(date +%s)"
  json="{\"orderId\":\"$1\",\"accountId\":\"$2\",\"groupId\":\"$3\",\"type\":\"$4\",\"seq\":$5,\"quantity\":${6:-0},\"price\":${7:-0},\"ts\":${now}000}"
  note "orders.events <- $4 seq=$5 ${6:+cumQty=$6}"
  echo "$3|$json" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic orders.events \
    --property parse.key=true --property key.separator='|' >/dev/null
}

redis_primary() { # prints the compose service that is currently the Redis primary
  docker compose exec -T redis-sentinel-1 redis-cli -p 26379 sentinel get-master-addr-by-name credit-redis \
    | head -1 | tr -d '\r'
}

redis_cli() { # redis_cli ARGS... (runs on the current primary)
  docker compose exec -T "$(redis_primary)" redis-cli "$@"
}

new_id() { echo "$1-$(date +%H%M%S)-$RANDOM"; }
