#!/usr/bin/env bash
# Continuous demo traffic: skewed across 20,000 accounts, full order lifecycles.
#   scripts/flow.sh start [RATE]   (default 20 orders/s)
#   scripts/flow.sh logs
#   scripts/flow.sh stop
source "$(dirname "$0")/lib.sh"

case "${1:-}" in
  start)
    docker rm -f credit-poc-flow >/dev/null 2>&1 || true
    docker compose run -d --name credit-poc-flow gateway-simulator --sim.mode=flow --sim.rate="${2:-20}"
    note "Started. Follow with: scripts/flow.sh logs"
    ;;
  logs) docker logs -f credit-poc-flow ;;
  stop) docker rm -f credit-poc-flow ;;
  *) echo "usage: $0 start [rate] | logs | stop"; exit 1 ;;
esac
