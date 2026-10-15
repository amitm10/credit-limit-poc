# Pre-Trade Credit Limit Service (POC)

A central pre-trade credit check for Global Markets order flow. Many stateless instances
approve trades concurrently without ever over-committing a client's account or group
credit limit, and exposure stays correct as orders are acknowledged, filled, cancelled
and amended, and when Redis fails over.

**Problem statement:** before a trade goes to market, confirm the client has headroom at
the account level and at the parent-group level, at 5,000+ checks per second with p99
under 5 ms, from many instances at once, never approving the same headroom twice, and
keep exposure right through the order lifecycle and through infrastructure failures.

## Why these three technologies

| | Owns | Why nothing simpler works here |
|---|---|---|
| **Spring REST** | The synchronous decision | The gateway blocks until it gets yes or no: request-response by nature. One contract for every gateway, the officer UI and the bank's API tooling. Spring Security enforces roles on every call. |
| **Redis** | Concurrent correctness | One Lua script checks account and group headroom and reserves at both, or neither, atomically across all instances. A local map cannot be shared; one SQL row lock per level serialises a busy fund manager's orders behind round trips. |
| **Kafka** | Durability, lifecycle, fan-out | Acks, fills and cancels from every gateway arrive in order per group; every decision is an event, so Redis can be rebuilt from Kafka at any time; risk and surveillance consume independently. |

## Architecture

```
 Gateway ──POST /credit-checks──► nginx ──► credit-check-api x3 ──Lua reserve + WAIT──► Redis primary ─► replica
 (simulator)                                      │                                        ▲ 3 Sentinels
     │                                            └──RESERVED/DECLINED──► credit.reservations (source of truth)
     └──ACK / FILL / CANCEL / AMEND──► orders.events ──► exposure-processor ──Lua apply──► Redis
                                                              ├── orphan sweeper (30 s, RESERVED only)
                                                              ├── rebuild Redis from Kafka (manual or on failover)
 limit-loader ──► credit.limits (compacted) + Redis           └── state changes ──► credit.reservations
```

## Modules

| Module | What it does |
|---|---|
| `credit-common` | DTOs, events, key scheme, money in cents, and the five Lua scripts that hold all the correctness |
| `credit-check-api` | REST check, amend, exposure and override endpoints; JWT roles; fail-closed 503s; circuit breaker |
| `exposure-processor` | Applies order events, expires orphans, rebuilds Redis from Kafka, reacts to Sentinel failover |
| `limit-loader` | Seeds 2,000 groups and 20,000 accounts into Kafka and Redis, then exits |
| `gateway-simulator` | Demo traffic (`flow`), the 500-way race (`concurrency`) and a fixed-rate load test (`load`) |

## Quick start (Mac)

Needs Docker Desktop (6 GB RAM), JDK 21 and Maven.

```bash
scripts/00-up.sh          # build, start, wait for ready (about 2 minutes the first time)
scripts/01-smoke.sh       # approve, reject at account level, reject at group level
scripts/02-concurrency.sh # 500 parallel checks at 2x headroom: zero over-commit
```

Full demo script, expected output and troubleshooting: [RUNBOOK.md](RUNBOOK.md).

## Key design decisions

- **Fail closed.** If the service cannot prove headroom it answers 503, never a guess.
- **Kafka is the source of truth; Redis is working state.** An approval is returned only
  after its RESERVED event is acknowledged by Kafka. If that publish fails, the reservation
  is released and the caller gets 503.
- **Idempotent everywhere.** Order ID is the check's idempotency key; order events are
  deduplicated by sequence number; fills carry cumulative quantity (like FIX CumQty), so a
  late or duplicate fill can never double-count.
- **Lag is conservative.** Exposure is reserved before the order leaves; later events only
  move it to used or release it. A slow processor can cause a false reject, never an over-commit.
- **WAIT on the writing connection.** Each check runs its reserve script and `WAIT 1 5` on
  one pooled connection, so WAIT confirms that reservation's replication.

## Deliberate simplifications

Exposure is notional in USD (no PFE, netting, collateral or FX). Two limit levels (account,
group). Self-signed dev JWTs. Single Kafka broker. Redis Cluster sharding is designed for
(hash tags) but not deployed. The rebuild holds the latest state per order in memory.

Built on Spring Boot 3.5.6, Java 21 (virtual threads), Kafka 3.9 (KRaft), Redis 7.4.
