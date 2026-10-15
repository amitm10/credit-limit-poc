#!/bin/sh
# Writes a Sentinel config (Sentinel must be able to rewrite its own file) and starts it.
# SENTINEL_HOST is this container's own hostname, announced so clients get resolvable names.
set -e
cat > /tmp/sentinel.conf <<EOF
port 26379
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip ${SENTINEL_HOST}
sentinel monitor credit-redis redis-primary 6379 2
sentinel down-after-milliseconds credit-redis 3000
sentinel failover-timeout credit-redis 10000
sentinel parallel-syncs credit-redis 1
EOF
exec redis-server /tmp/sentinel.conf --sentinel
