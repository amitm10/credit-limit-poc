package com.poc.credit.processor.service;

import com.poc.credit.common.events.OrderEvent;
import com.poc.credit.common.events.ReservationEvent;
import com.poc.credit.common.redis.CreditScripts;
import com.poc.credit.common.redis.Keys;
import com.poc.credit.common.redis.ScriptResult;
import com.poc.credit.processor.config.ProcessorProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Applies one order event: atomically in Redis (apply_event.lua), then publishes the new
 * reservation state. A duplicate re-publishes the current state, which makes a crash
 * between the Redis write and the publish self-healing on redelivery.
 */
@Service
public class EventApplier {

    private static final Logger log = LoggerFactory.getLogger(EventApplier.class);

    private final CreditScripts scripts;
    private final StringRedisTemplate redis;
    private final ReservationPublisher publisher;
    private final ProcessorProperties props;
    private final MeterRegistry registry;
    private final Timer latency;

    public EventApplier(CreditScripts scripts, StringRedisTemplate redis, ReservationPublisher publisher,
                        ProcessorProperties props, MeterRegistry registry) {
        this.scripts = scripts;
        this.redis = redis;
        this.publisher = publisher;
        this.props = props;
        this.registry = registry;
        this.latency = Timer.builder("credit.event.apply.latency")
                .publishPercentiles(0.5, 0.99)
                .register(registry);
    }

    public void apply(OrderEvent event) {
        latency.record(() -> doApply(event));
    }

    private void doApply(OrderEvent event) {
        String groupId = event.groupId() != null ? event.groupId() : redis.opsForValue().get(Keys.accountGroup(event.accountId()));
        if (groupId == null) {
            count(event, "UNKNOWN_ACCOUNT");
            log.warn("Order event for unknown account {} (order {})", event.accountId(), event.orderId());
            return;
        }

        ScriptResult r = scripts.applyEvent(groupId, event.accountId(), event.orderId(), event.type(),
                event.seq(), event.amountCents(), System.currentTimeMillis(), props.terminalTtl());
        count(event, r.status());

        switch (r.status()) {
            case "APPLIED", "DUPLICATE" -> {
                if (!r.detail().isEmpty() && r.is("APPLIED")) {
                    registry.counter("credit.alerts", "type", r.detail()).increment();
                    log.warn("ALERT {} on order {} account {}: {}", r.detail(), event.orderId(), event.accountId(),
                            r.reservation());
                }
                String alert = r.is("APPLIED") ? r.detail() : "";
                publisher.publish(ReservationEvent.fromHash(r.reservation(), ReservationEvent.SOURCE_ORDER_EVENT, alert));
            }
            case "UNKNOWN_ORDER" -> log.warn("Order event {} for unknown order {} (never checked, or expired from Redis)",
                    event.type(), event.orderId());
            case "IGNORED" -> log.debug("Ignored {} for order {} in state {}", event.type(), event.orderId(), r.detail());
            default -> log.error("Unexpected apply status {} for {}", r.status(), event);
        }
    }

    private void count(OrderEvent event, String status) {
        registry.counter("credit.order.events", "type", event.type().name(), "status", status).increment();
    }
}
