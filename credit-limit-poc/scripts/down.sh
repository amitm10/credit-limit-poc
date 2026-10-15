#!/usr/bin/env bash
# Stops everything and removes containers and data (Kafka topics, Redis).
source "$(dirname "$0")/lib.sh"
docker rm -f credit-poc-flow >/dev/null 2>&1 || true
docker compose --profile sim down -v --remove-orphans
