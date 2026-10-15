package com.poc.credit.api.service;

import com.poc.credit.common.Money;
import com.poc.credit.common.api.AmendRequest;
import com.poc.credit.common.api.AmendResponse;
import com.poc.credit.common.api.CreditCheckResponse;
import com.poc.credit.common.events.ReservationEvent;
import com.poc.credit.common.model.Decision;
import com.poc.credit.common.redis.CreditScripts;
import com.poc.credit.common.redis.ScriptResult;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Amend pre-check. Only an increase needs headroom, and only the delta is reserved.
 * A decrease is approved at once but released only when the venue confirms it, so exposure
 * is never understated while the amend is in flight.
 *
 * <p>If the publish fails after an amend-up, the caller gets 503 and the delta stays reserved
 * (conservative) until the order closes or the same amend is retried and replayed.
 */
@Service
public class AmendService {

    private static final Set<String> LEVELS = Set.of("ACCOUNT", "GROUP");

    private final StringRedisTemplate redis;
    private final CreditScripts scripts;
    private final GroupResolver groups;
    private final EventPublisher publisher;
    private final ReplicaWaiter replicas;
    private final MaintenanceMode maintenance;
    private final CircuitBreaker breaker;

    public AmendService(StringRedisTemplate redis, CreditScripts scripts, GroupResolver groups,
                        EventPublisher publisher, ReplicaWaiter replicas, MaintenanceMode maintenance,
                        CircuitBreaker breaker) {
        this.redis = redis;
        this.scripts = scripts;
        this.groups = groups;
        this.publisher = publisher;
        this.replicas = replicas;
        this.maintenance = maintenance;
        this.breaker = breaker;
    }

    public AmendResponse amend(String orderId, AmendRequest req) {
        maintenance.ensureAvailable();
        return breaker.executeSupplier(() -> doAmend(orderId, req));
    }

    private AmendResponse doAmend(String orderId, AmendRequest req) {
        String groupId = groups.groupOf(req.accountId());
        long newTotal = Money.notionalCents(req.newQuantity(), req.newPrice());

        ScriptResult r = redis.execute(new SessionCallback<ScriptResult>() {
            @Override
            public <K, V> ScriptResult execute(RedisOperations<K, V> operations) throws DataAccessException {
                ScriptResult res = scripts.amend(groupId, req.accountId(), orderId, req.amendId(), newTotal,
                        System.currentTimeMillis());
                if (res.is("APPROVED") && "UP".equals(res.detail())) {
                    replicas.awaitReplication();
                }
                return res;
            }
        });

        switch (r.status()) {
            case "APPROVED", "DECLINED", "REPLAY" -> {
                publisher.publish(ReservationEvent.fromHash(r.reservation(), ReservationEvent.SOURCE_AMEND, ""));
                boolean rejected = LEVELS.contains(r.detail());
                return new AmendResponse(rejected ? Decision.REJECTED : Decision.APPROVED, orderId,
                        rejected ? null : r.detail(), Money.toUsd(r.longField("amountCents")),
                        new CreditCheckResponse.Headroom(Money.toUsd(r.accountHeadroomCents()),
                                Money.toUsd(r.groupHeadroomCents())),
                        rejected ? r.detail() : null);
            }
            case "UNKNOWN_ORDER" -> throw new Errors.NotFoundException("No reservation for order " + orderId);
            case "ORDER_CLOSED" -> throw new Errors.ConflictException("Order " + orderId + " is " + r.detail());
            case "AMEND_PENDING" -> throw new Errors.ConflictException(
                    "Order " + orderId + " already has an amend awaiting venue confirmation");
            default -> throw new IllegalStateException("Unexpected amend status " + r.status());
        }
    }
}
