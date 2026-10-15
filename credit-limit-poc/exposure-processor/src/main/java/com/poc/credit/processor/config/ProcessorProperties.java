package com.poc.credit.processor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param terminalTtl         how long closed reservations stay in Redis (idempotent replays)
 * @param expiredTtl          how long expired orphans stay, so a very late ACK can revive them
 * @param orphanTimeout       a RESERVED order with no ACK after this long is released
 * @param publishTimeout      how long to wait for Kafka to acknowledge a state change
 * @param reconcileOnFailover rebuild Redis from Kafka when Sentinel promotes a new primary
 * @param failoverRebuildDelay pause before that rebuild, to let clients reconnect
 */
@ConfigurationProperties(prefix = "credit")
public record ProcessorProperties(@DefaultValue("10m") Duration terminalTtl,
                                  @DefaultValue("24h") Duration expiredTtl,
                                  @DefaultValue("30s") Duration orphanTimeout,
                                  @DefaultValue("2s") Duration publishTimeout,
                                  @DefaultValue("false") boolean reconcileOnFailover,
                                  @DefaultValue("3s") Duration failoverRebuildDelay) {
}
