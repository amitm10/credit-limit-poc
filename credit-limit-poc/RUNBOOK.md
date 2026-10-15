# Runbook

Everything runs on one Mac with Docker. All commands are run from the project root.

## 1. Prerequisites

| Tool | Check | Install |
|---|---|---|
| Docker Desktop | `docker compose version` | docker.com; Settings > Resources: 6 GB memory, 4 CPUs |
| JDK 21 | `java -version` | `brew install openjdk@21` |
| Maven 3.9+ | `mvn -version` | `brew install maven` |
| jq (optional) | `jq --version` | `brew install jq` (scripts fall back to python3) |

Ports used on your Mac: **8080** (API via nginx), **8081** (processor admin), **29092** (Kafka).

## 2. Build and start

```bash
chmod +x scripts/*.sh
scripts/00-up.sh
```

This runs `mvn clean package -DskipTests`, then `docker compose up -d --build`, and waits
until the three API instances and the processor report ready. First run downloads images
and dependencies (a few minutes); later starts take about 60 to 90 seconds.

What starts:

| Container | Role |
|---|---|
| kafka, kafka-init | Single KRaft broker; topics created once |
| redis-primary, redis-replica, redis-sentinel-1..3 | Redis with automatic failover |
| limit-loader | Seeds 2,000 groups / 20,000 accounts, then exits |
| credit-api-1..3 | The check API, stateless |
| nginx | Load-balances the API on port 8080 |
| exposure-processor | Order events, orphan sweeper, rebuild; admin on 8081 |

Check status: `docker compose ps`. Logs: `docker compose logs -f credit-api-1 exposure-processor`.

## 3. Run the tests (optional, needs Docker running)

```bash
mvn verify
```

Unit tests, the Lua scripts against a real Redis (Testcontainers), API integration tests
with an embedded Kafka broker, the 500-thread HTTP concurrency test, the compensation test
(Kafka refuses the event: 503 and nothing left reserved), and the processor lifecycle and
rebuild tests.

## 4. Demo script (about 20 minutes)

Open three terminals. Terminal 1 runs the steps; terminal 2 shows live traffic
(`scripts/flow.sh logs`); terminal 3 shows the processor (`docker compose logs -f exposure-processor`).
Scripts pause between steps; set `NO_PAUSE=1` to run straight through.

### Step 1: approve, reject at account level, reject at group level

```bash
scripts/01-smoke.sh
```

Expected: `APPROVED` with headroom at both levels; `REJECTED` with `failedLevel: ACCOUNT`;
then a credit officer override on ACC-00002 to $500M, and a $150M order that is `REJECTED`
with `failedLevel: GROUP` because its manager G0000 is capped at $100M.

**Say:** the caller never sends its group or limits; the service looks them up. A
rejection is a 200 with a decision, not an error.

### Step 2: the concurrency race (the Redis proof)

```bash
scripts/02-concurrency.sh
```

Expected:

```
Headroom before      $10,000,000.00
Requests             500 in parallel, $40,000.00 each
Approved             250 (maximum that fits: 250)
Rejected             250
Over-commit          $0.00
RESULT               PASS: never over-committed, Redis matches the approvals exactly
```

**Say:** 500 requests released through a start gate across three instances. One Lua
script per check, executed atomically by Redis: no locks, no retries, no race window. The
approved orders are cancelled afterwards, so wait a few seconds before re-running.

### Step 3: start live traffic

```bash
scripts/flow.sh start 20     # 20 orders/s, skewed: 10% to the mega fund G0000
scripts/flow.sh logs         # in terminal 2
```

Every 5 s it prints approvals, rejections by level, 503s and latency. After a minute or
two, group-level rejections appear as the mega fund G0000 fills its $100M group limit.

### Step 4: one order's lifecycle

```bash
scripts/03-lifecycle.sh
```

Check ($1M reserved) → ACK (WORKING) → partial FILL (cumulative 4,000: $400k reserved →
used) → the same FILL again (no change) → CANCEL ($600k released).

**Say:** fills carry cumulative quantity like FIX CumQty, so duplicates and out-of-order
fills are harmless. Reservations are released by events, never by a timer, except orphans.

### Step 5: idempotency

```bash
scripts/04-idempotency.sh
```

Same order ID and payload twice: same answer, reserved once. Same ID with a different
payload: `409 Conflict`.

### Step 6: kill an API instance under load

```bash
scripts/05-kill-api-pod.sh
```

Terminal 2 keeps flowing with no errors (or a single one). nginx retries the in-flight
POST on another instance, which is safe because checks are idempotent.

### Step 7: kill the Redis primary under load

```bash
scripts/06-redis-failover.sh
```

Expected: a few seconds of 503s in terminal 2 (fail closed), `Sentinel promoted
redis-replica after ~4-6 s`, then in terminal 3 `REBUILD started (sentinel-failover)` and
`REBUILD complete`, then approvals resume. Exposure for ACC-00001 is intact.

**Say:** replication is asynchronous, so a promoted replica can miss the last writes. Each
check waits up to 5 ms for the replica (`WAIT 1 5`), and on failover the processor rebuilds
from Kafka, so nothing approved is lost.

Until the old primary is restarted, the new primary has no replica, so every check's
`WAIT` times out after 5 ms: latency rises by about 5 ms and `credit_redis_wait_timeouts_total`
climbs. That is the honest cost of running without a replica, and a good thing to point out.
Wait about 10 s after the restart (the old primary resyncs as a replica) before running
this step again.

### Step 8: rebuild Redis from Kafka alone

```bash
scripts/flow.sh stop
scripts/07-rebuild.sh
```

Takes a snapshot of several accounts, runs `FLUSHALL`, calls `POST /admin/rebuild`, and
compares. Expected: `MATCH: Redis rebuilt exactly from Kafka`.

### Step 9: processor down

```bash
scripts/08-processor-lag.sh
```

With the processor stopped, checks still work; a cancelled order's exposure stays reserved
(conservative). On restart the processor catches up and releases it.

### Optional: load test

```bash
scripts/load-test.sh 2000 60     # rate, seconds
```

Prints achieved throughput, approvals and rejections by level, 503s, and client-side
p50/p90/p99/p99.9 latency. On a laptop running every container plus the load generator,
expect roughly 2,000 to 3,000 checks/s. The 5,000/s target is a production design number;
the laptop run shows the latency curve and zero over-commit at whatever rate it reaches.

## 5. Useful commands

```bash
# Token for curl (dev profile only)
TOKEN=$(curl -s -X POST 'localhost:8080/dev/token?role=CREDIT_OFFICER' | sed 's/.*"token":"\([^"]*\)".*/\1/')
curl -s localhost:8080/api/v1/exposure/ACC-00001 -H "Authorization: Bearer $TOKEN"

# Which Redis is primary right now
docker compose exec redis-sentinel-1 redis-cli -p 26379 sentinel get-master-addr-by-name credit-redis

# Processor status / manual rebuild
curl -s localhost:8081/admin/status
curl -s -X POST localhost:8081/admin/rebuild

# Metrics (Prometheus format)
curl -s localhost:8081/actuator/prometheus | grep -E 'credit_(alerts|orphans|order_events)'
docker compose exec credit-api-1 curl -s localhost:8080/actuator/prometheus | grep credit_check

# Consumer lag
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group exposure-processor

# Watch decisions on the source-of-truth topic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic credit.reservations --max-messages 5
```

Key metrics: `credit_check_latency_seconds` (p50/p99/p99.9), `credit_check_decisions_total`
by decision and failed level, `credit_redis_wait_timeouts_total`, `credit_check_compensations_total`,
`credit_alerts_total` (LATE_ACK, LATE_FILL, OVERFILL, UNCHECKED_INCREASE),
`credit_orphans_expired_total`, `resilience4j_circuitbreaker_state`, and Kafka consumer lag.

## 6. Stop

```bash
scripts/flow.sh stop
scripts/down.sh          # removes containers and all data
```

## 7. Troubleshooting

| Symptom | Fix |
|---|---|
| `mvn` cannot resolve `spring-boot-starter-parent:3.5.6` | Change the parent version in `pom.xml` to the latest 3.5.x release |
| API containers restart in a loop | `docker compose logs credit-api-1`. Usually Kafka or Sentinel not ready yet; they recover on their own within a minute |
| `Cannot get a token` from scripts | The stack is not up or the API is not ready: `docker compose ps` |
| Every check returns 503 | `curl -s localhost:8081/admin/status`. A failed rebuild leaves maintenance on by design: run `curl -X POST localhost:8081/admin/rebuild` |
| Concurrency test says "No headroom left" | The previous run's cancels have not applied yet; wait 5 s |
| `redis-replica` or `redis-primary` missing after the failover demo | `docker compose start redis-primary redis-replica` |
| Port 8080 already in use | Stop the other process, or change `8080:8080` under nginx in `docker-compose.yml` |
| Docker runs out of memory | Docker Desktop > Resources > Memory 6 GB, or stop `credit-api-3` |

## 8. Where the code is

| Concern | File |
|---|---|
| Atomic check-and-reserve | `credit-common/src/main/resources/lua/reserve.lua` |
| Lifecycle, cumulative fills, late ACK | `credit-common/src/main/resources/lua/apply_event.lua` |
| Amend pre-check | `credit-common/src/main/resources/lua/amend.lua` |
| Orphan expiry / compensation | `expire.lua`, `release.lua` |
| Check flow, WAIT, publish, compensation | `credit-check-api/.../service/CreditCheckService.java` |
| Fail-closed error mapping | `credit-check-api/.../web/ApiExceptionHandler.java` |
| Roles | `credit-check-api/.../config/SecurityConfig.java` |
| Event consumer, retries | `exposure-processor/.../service/OrderEventListener.java`, `config/ProcessorConfig.java` |
| Rebuild from Kafka | `exposure-processor/.../service/RebuildService.java` |
| Sentinel failover hook | `exposure-processor/.../service/FailoverListener.java` |
| Concurrency and load tests | `gateway-simulator/.../Simulator.java` |
