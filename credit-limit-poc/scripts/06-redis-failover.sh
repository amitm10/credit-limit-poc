#!/usr/bin/env bash
# Demo step 6: kill the Redis primary under live traffic.
# Expect: a few seconds of 503 (fail closed), Sentinel promotes the replica, the processor
# rebuilds from Kafka to recover any writes the replica missed, then checks resume.
# Run scripts/flow.sh start first; watch scripts/flow.sh logs and
# docker compose logs -f exposure-processor in other terminals.
source "$(dirname "$0")/lib.sh"
tokens

PRIMARY="$(redis_primary)"
if [ -z "$PRIMARY" ]; then echo "Cannot find the Redis primary via Sentinel" >&2; exit 1; fi
OTHER=redis-replica; [ "$PRIMARY" = "redis-replica" ] && OTHER=redis-primary

bold "Exposure for ACC-00001 (hot group) before"
exposure ACC-00001

bold "Current primary: $PRIMARY. Killing it."
docker compose kill "$PRIMARY"
START=$(date +%s)
note "Sentinel needs 3 s to agree the primary is down, then promotes $OTHER."
for _ in $(seq 1 30); do
  sleep 1
  NOW="$(redis_primary)"
  if [ "$NOW" = "$OTHER" ]; then
    bold "Sentinel promoted $OTHER after $(( $(date +%s) - START )) s"
    break
  fi
done
note "The processor now rebuilds from Kafka (logs: REBUILD started / complete)."
sleep 8
curl -s "$PROC/admin/status" | pretty

bold "Exposure for ACC-00001 after failover and rebuild"
exposure ACC-00001
pause

bold "Restarting $PRIMARY: it rejoins as a replica of $OTHER"
docker compose start "$PRIMARY"
note "Wait ~10 s for it to resync before running this demo again."
