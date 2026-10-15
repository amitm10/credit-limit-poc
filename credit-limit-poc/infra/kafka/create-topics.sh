#!/bin/sh
# Creates the POC topics once Kafka is up. Safe to re-run.
set -eu
BOOTSTRAP=kafka:9092
TOPICS=/opt/kafka/bin/kafka-topics.sh

create() {
  "$TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists "$@"
}

# Order lifecycle events from gateways. Keyed by groupId: one group's events stay in order.
create --topic orders.events --partitions 24 --replication-factor 1 \
  --config retention.ms=604800000

# Every credit decision and state change: the source of truth for rebuilding Redis.
create --topic credit.reservations --partitions 24 --replication-factor 1 \
  --config retention.ms=604800000

# Limit definitions and overrides, latest value per key.
create --topic credit.limits --partitions 6 --replication-factor 1 \
  --config cleanup.policy=compact

"$TOPICS" --bootstrap-server "$BOOTSTRAP" --list
echo "Topics ready."
