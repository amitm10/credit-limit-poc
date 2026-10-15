package com.poc.credit.api.service;

import com.poc.credit.common.redis.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * While the exposure processor rebuilds Redis from Kafka it sets a maintenance key, and every
 * API instance fails closed. The flag is polled every 250 ms rather than read per request,
 * to keep the check path to a single Redis round trip.
 */
@Component
public class MaintenanceMode {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceMode.class);

    private final StringRedisTemplate redis;
    private volatile boolean active;

    public MaintenanceMode(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Scheduled(fixedDelay = 250)
    void poll() {
        try {
            boolean now = Boolean.TRUE.equals(redis.hasKey(Keys.MAINTENANCE));
            if (now != active) {
                log.warn("Maintenance mode {}", now ? "ON: rejecting checks with 503" : "OFF: accepting checks");
            }
            active = now;
        } catch (RuntimeException e) {
            log.debug("Maintenance poll failed: {}", e.getMessage());
        }
    }

    public void ensureAvailable() {
        if (active) {
            throw new Errors.ServiceUnavailableException("Exposure rebuild in progress");
        }
    }
}
