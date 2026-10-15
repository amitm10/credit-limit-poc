package com.poc.credit.sim;

import com.poc.credit.common.Json;
import com.poc.credit.common.Topics;
import com.poc.credit.common.api.AmendRequest;
import com.poc.credit.common.api.AmendResponse;
import com.poc.credit.common.events.OrderEvent;
import com.poc.credit.common.model.Decision;
import com.poc.credit.common.model.OrderEventType;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Plays the venue for an approved order: ACK, then fills, cancels or an amend, published
 * to orders.events exactly as a real gateway would. Fills carry cumulative quantity.
 *
 * <pre>
 *   orphanPercent  no ACK ever (sweeper releases after 30 s)
 *   55%            ACK, then filled in one or two steps
 *   25%            ACK, partial fill, then cancel
 *   15%            ACK, then cancel
 *    5%            ACK, amend up 20% via REST, venue confirms (80%) or rejects, then filled
 * </pre>
 */
public class OrderLifecycle {

    public record Order(String orderId, String accountId, String groupId, long quantity, BigDecimal price) {
    }

    private final KafkaTemplate<String, String> kafka;
    private final CreditApiClient api;
    private final int orphanPercent;
    private final long stepMillis;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final AtomicLong events = new AtomicLong();
    private final AtomicLong orphans = new AtomicLong();
    private final AtomicLong amends = new AtomicLong();

    public OrderLifecycle(KafkaTemplate<String, String> kafka, CreditApiClient api, int orphanPercent, long stepMillis) {
        this.kafka = kafka;
        this.api = api;
        this.orphanPercent = orphanPercent;
        this.stepMillis = stepMillis;
    }

    public void start(Order o) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        if (r.nextInt(100) < orphanPercent) {
            orphans.incrementAndGet();
            return;
        }
        int roll = r.nextInt(100);
        later(1, () -> {
            publish(o, OrderEventType.ACK, 1, 0);
            if (roll < 55) {
                long half = o.quantity() / 2;
                if (half > 0 && r.nextBoolean()) {
                    later(1, () -> publish(o, OrderEventType.FILL, 2, half));
                    later(2, () -> publish(o, OrderEventType.FILL, 3, o.quantity()));
                } else {
                    later(1, () -> publish(o, OrderEventType.FILL, 2, o.quantity()));
                }
            } else if (roll < 80) {
                long part = Math.max(1, o.quantity() * (30 + r.nextInt(41)) / 100);
                later(1, () -> publish(o, OrderEventType.FILL, 2, part));
                later(2, () -> publish(o, OrderEventType.CANCEL, 3, 0));
            } else if (roll < 95) {
                later(1, () -> publish(o, OrderEventType.CANCEL, 2, 0));
            } else {
                later(1, () -> amendUp(o));
            }
        });
    }

    private void amendUp(Order o) {
        amends.incrementAndGet();
        long newQty = o.quantity() * 12 / 10;
        AmendRequest req = new AmendRequest(o.orderId() + "-A1", o.accountId(), newQty, o.price());
        CreditApiClient.Result<AmendResponse> res = api.amend(o.orderId(), req);
        if (!res.ok() || res.body().decision() != Decision.APPROVED) {
            publish(o, OrderEventType.CANCEL, 2, 0);
            return;
        }
        Order amended = new Order(o.orderId(), o.accountId(), o.groupId(), newQty, o.price());
        if (ThreadLocalRandom.current().nextInt(100) < 80) {
            publish(amended, OrderEventType.AMEND_CONFIRMED, 2, newQty);
            later(1, () -> publish(amended, OrderEventType.FILL, 3, newQty));
        } else {
            publish(o, OrderEventType.AMEND_REJECTED, 2, 0);
            later(1, () -> publish(o, OrderEventType.FILL, 3, o.quantity()));
        }
    }

    /** Publishes one event for the order, keyed by group like a real gateway. */
    public void publish(Order o, OrderEventType type, long seq, long quantity) {
        OrderEvent e = new OrderEvent(o.orderId(), o.accountId(), o.groupId(), type, seq, quantity, o.price(),
                System.currentTimeMillis());
        kafka.send(Topics.ORDER_EVENTS, o.groupId(), Json.write(e));
        events.incrementAndGet();
    }

    private void later(int steps, Runnable action) {
        long jitter = ThreadLocalRandom.current().nextLong(stepMillis / 2 + 1);
        try {
            scheduler.schedule(action, steps * stepMillis + jitter, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException shuttingDown) {
            // The run is over; remaining lifecycle steps are dropped (the sweeper tidies orphans).
        }
    }

    public long events() {
        return events.get();
    }

    public long orphans() {
        return orphans.get();
    }

    public long amends() {
        return amends.get();
    }

    /** Lets in-flight lifecycles finish for a while, then stops and flushes the producer. */
    public void drain(long waitMillis) throws InterruptedException {
        Thread.sleep(waitMillis);
        scheduler.shutdownNow();
        kafka.flush();
    }
}
