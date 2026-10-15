package com.poc.credit.processor.service;

import com.poc.credit.common.events.ReservationEvent;
import com.poc.credit.common.redis.CreditScripts;
import com.poc.credit.common.redis.Keys;
import com.poc.credit.common.redis.ScriptResult;
import com.poc.credit.processor.config.ProcessorProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Releases reservations that were approved but never acknowledged by the venue (the gateway
 * crashed, or the order never left). Only RESERVED orders qualify; working orders, including
 * GTC orders resting for days, are released only by cancel or reject events.
 *
 * <p>Safe to run on several processor instances at once: expire.lua re-checks the state, so
 * a second attempt is a no-op.
 */
@Component
public class OrphanSweeper {

    private static final Logger log = LoggerFactory.getLogger(OrphanSweeper.class);
    private static final long BATCH = 200;

    private final StringRedisTemplate redis;
    private final CreditScripts scripts;
    private final ReservationPublisher publisher;
    private final ProcessorProperties props;
    private final Counter expired;
    private volatile boolean paused;

    public OrphanSweeper(StringRedisTemplate redis, CreditScripts scripts, ReservationPublisher publisher,
                         ProcessorProperties props, MeterRegistry registry) {
        this.redis = redis;
        this.scripts = scripts;
        this.publisher = publisher;
        this.props = props;
        this.expired = Counter.builder("credit.orphans.expired").register(registry);
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }

    @Scheduled(fixedDelayString = "${credit.sweep-interval-ms:5000}", initialDelay = 10_000)
    public void sweep() {
        if (paused) {
            return;
        }
        try {
            long cutoff = System.currentTimeMillis() - props.orphanTimeout().toMillis();
            Set<String> groups = redis.opsForSet().members(Keys.GROUPS_INDEX);
            if (groups == null) {
                return;
            }
            int released = 0;
            for (String groupId : groups) {
                Set<String> due = redis.opsForZSet().rangeByScore(Keys.pending(groupId), 0, cutoff, 0, BATCH);
                if (due == null) {
                    continue;
                }
                for (String orderId : due) {
                    released += expire(groupId, orderId, cutoff) ? 1 : 0;
                }
            }
            if (released > 0) {
                log.info("Orphan sweep released {} unacknowledged reservations", released);
            }
        } catch (RuntimeException e) {
            log.warn("Orphan sweep failed, will retry: {}", e.getMessage());
        }
    }

    private boolean expire(String groupId, String orderId, long cutoff) {
        Object accountId = redis.opsForHash().get(Keys.reservation(groupId, orderId), "accountId");
        if (accountId == null) {
            redis.opsForZSet().remove(Keys.pending(groupId), orderId);
            return false;
        }
        ScriptResult r = scripts.expire(groupId, (String) accountId, orderId, cutoff, props.expiredTtl());
        if (!r.is("EXPIRED")) {
            return false;
        }
        expired.increment();
        publisher.publish(ReservationEvent.fromHash(r.reservation(), ReservationEvent.SOURCE_SWEEPER, ""));
        return true;
    }
}
