package com.poc.credit.api.service;

import com.poc.credit.api.config.CreditProperties;
import com.poc.credit.common.Money;
import com.poc.credit.common.api.CreditCheckRequest;
import com.poc.credit.common.api.CreditCheckResponse;
import com.poc.credit.common.api.RequestHash;
import com.poc.credit.common.events.ReservationEvent;
import com.poc.credit.common.model.Decision;
import com.poc.credit.common.model.ReservationState;
import com.poc.credit.common.redis.CreditScripts;
import com.poc.credit.common.redis.ScriptResult;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;

/**
 * The synchronous check:
 * <ol>
 *   <li>Resolve the account's group (never taken from the caller).</li>
 *   <li>One Lua script checks both levels and reserves at both, or neither.</li>
 *   <li>WAIT for a replica on the same connection (when configured).</li>
 *   <li>Publish the decision to Kafka and wait for the ack.</li>
 *   <li>Only then answer. If the publish fails, release the reservation and answer 503.</li>
 * </ol>
 */
@Service
public class CreditCheckService {

    private static final Logger log = LoggerFactory.getLogger(CreditCheckService.class);

    private final StringRedisTemplate redis;
    private final CreditScripts scripts;
    private final GroupResolver groups;
    private final EventPublisher publisher;
    private final ReplicaWaiter replicas;
    private final MaintenanceMode maintenance;
    private final CircuitBreaker breaker;
    private final CreditProperties props;
    private final MeterRegistry registry;
    private final Timer latency;

    public CreditCheckService(StringRedisTemplate redis, CreditScripts scripts, GroupResolver groups,
                              EventPublisher publisher, ReplicaWaiter replicas, MaintenanceMode maintenance,
                              CircuitBreaker breaker, CreditProperties props, MeterRegistry registry) {
        this.redis = redis;
        this.scripts = scripts;
        this.groups = groups;
        this.publisher = publisher;
        this.replicas = replicas;
        this.maintenance = maintenance;
        this.breaker = breaker;
        this.props = props;
        this.registry = registry;
        this.latency = Timer.builder("credit.check.latency")
                .description("Server-side credit check latency")
                .publishPercentiles(0.5, 0.99, 0.999)
                .publishPercentileHistogram()
                .register(registry);
    }

    public CreditCheckResponse check(CreditCheckRequest req) {
        maintenance.ensureAvailable();
        Supplier<CreditCheckResponse> guarded = () -> breaker.executeSupplier(() -> doCheck(req));
        return latency.record(guarded);
    }

    private CreditCheckResponse doCheck(CreditCheckRequest req) {
        String groupId = groups.groupOf(req.accountId());
        long cents = Money.notionalCents(req.quantity(), req.price());
        String hash = RequestHash.of(req);

        ScriptResult result = reserveAndReplicate(groupId, req, cents, hash);

        return switch (result.status()) {
            case "APPROVED" -> {
                ReservationEvent event = ReservationEvent.fromHash(result.reservation(), ReservationEvent.SOURCE_CHECK, "");
                try {
                    publisher.publish(event);
                } catch (PublishFailedException e) {
                    compensate(groupId, req, hash);
                    throw e;
                }
                yield respond(Decision.APPROVED, req.orderId(), cents, result, null);
            }
            case "DECLINED" -> {
                // Every decision is published. If this fails the caller gets 503 and a retry
                // replays the stored rejection and publishes it again.
                publisher.publish(ReservationEvent.fromHash(result.reservation(), ReservationEvent.SOURCE_CHECK, ""));
                yield respond(Decision.REJECTED, req.orderId(), cents, result, result.detail());
            }
            case "REPLAY" -> {
                // Re-publish the current state: covers a crash between the reserve and the publish.
                publisher.publish(ReservationEvent.fromHash(result.reservation(), ReservationEvent.SOURCE_CHECK, ""));
                boolean declined = ReservationState.DECLINED.name().equals(result.field("state"));
                yield declined
                        ? respond(Decision.REJECTED, req.orderId(), cents, result, result.detail())
                        : respond(Decision.APPROVED, req.orderId(), cents, result, null);
            }
            case "CONFLICT" -> throw new Errors.ConflictException(
                    "Order ID " + req.orderId() + " was already used for a different order");
            case "NO_LIMIT" -> throw new Errors.NoLimitException(
                    "No " + result.detail() + " limit loaded for account " + req.accountId());
            default -> throw new IllegalStateException("Unexpected reserve status " + result.status());
        };
    }

    /**
     * Runs the reserve script and WAIT in one Redis session, so both use the same pooled
     * connection and WAIT confirms this reservation's replication.
     */
    private ScriptResult reserveAndReplicate(String groupId, CreditCheckRequest req, long cents, String hash) {
        return redis.execute(new SessionCallback<ScriptResult>() {
            @Override
            public <K, V> ScriptResult execute(RedisOperations<K, V> operations) throws DataAccessException {
                ScriptResult r = scripts.reserve(groupId, req.accountId(), req.orderId(), cents, hash,
                        System.currentTimeMillis(), props.terminalTtl());
                if (r.is("APPROVED")) {
                    replicas.awaitReplication();
                }
                return r;
            }
        });
    }

    /** Undo the reservation so a retry gets a fresh check. If Redis is down too, the sweeper releases it. */
    private void compensate(String groupId, CreditCheckRequest req, String hash) {
        try {
            ScriptResult released = scripts.release(groupId, req.accountId(), req.orderId(), hash);
            registry.counter("credit.check.compensations", "result", released.status()).increment();
            if (released.is("RELEASED")) {
                try {
                    publisher.publish(ReservationEvent.fromHash(released.reservation(),
                            ReservationEvent.SOURCE_COMPENSATION, ""));
                } catch (PublishFailedException ignored) {
                    // Best effort: Kafka is down anyway. Rebuild skips compensated orders it cannot see.
                }
            }
        } catch (RuntimeException e) {
            log.error("Compensation failed for order {}; the orphan sweeper will release it", req.orderId(), e);
        }
    }

    private CreditCheckResponse respond(Decision decision, String orderId, long cents, ScriptResult r, String failedLevel) {
        registry.counter("credit.check.decisions", "decision", decision.name(),
                "failedLevel", failedLevel == null ? "NONE" : failedLevel).increment();
        return new CreditCheckResponse(decision, orderId, Money.toUsd(cents),
                new CreditCheckResponse.Headroom(Money.toUsd(r.accountHeadroomCents()), Money.toUsd(r.groupHeadroomCents())),
                failedLevel);
    }
}
