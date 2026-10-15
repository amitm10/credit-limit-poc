package com.poc.credit.api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param jwtSecret           shared HS256 secret for POC tokens (at least 32 bytes)
 * @param terminalTtl         how long closed reservations stay in Redis for idempotent replays
 * @param kafkaPublishTimeout how long a check waits for Kafka to acknowledge the decision
 * @param waitReplicas        replicas that must confirm a reservation (Redis WAIT); 0 disables
 * @param waitTimeout         WAIT timeout; a timeout is counted and logged, never fails the check
 */
@ConfigurationProperties(prefix = "credit")
public record CreditProperties(String jwtSecret,
                               @DefaultValue("10m") Duration terminalTtl,
                               @DefaultValue("250ms") Duration kafkaPublishTimeout,
                               @DefaultValue("0") int waitReplicas,
                               @DefaultValue("5ms") Duration waitTimeout) {
}
