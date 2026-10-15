#!/usr/bin/env bash
# Builds the jars and images, starts the stack and waits until everything is ready.
set -e
source "$(dirname "$0")/lib.sh"

bold "1/3 Building jars (mvn clean package -DskipTests)"
mvn -q clean package -DskipTests

bold "2/3 Starting Kafka, Redis + Sentinel, 3 API instances, processor, nginx"
docker compose up -d --build

bold "3/3 Waiting for services to become ready"
for svc in credit-api-1 credit-api-2 credit-api-3 exposure-processor; do
  printf '  %-20s' "$svc"
  for _ in $(seq 1 90); do
    status="$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q "$svc")" 2>/dev/null || true)"
    [ "$status" = "healthy" ] && break
    sleep 2
  done
  echo "$status"
done

printf '  %-20s' "limit-loader"
for _ in $(seq 1 60); do
  if docker compose logs limit-loader 2>/dev/null | grep -q -E "Loaded .* accounts|already loaded"; then
    echo "limits loaded"; break
  fi
  sleep 2
done

bold "Ready. API: $API (via nginx)   Processor admin: $PROC"
note "Next: scripts/01-smoke.sh"
