package com.poc.credit.sim;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param mode           flow (continuous demo traffic), concurrency (the 500-way race) or load
 * @param apiUrl         the API behind nginx
 * @param rate           orders per second (flow and load)
 * @param duration       how long load mode runs
 * @param minNotional    smallest order, USD
 * @param maxNotional    largest order, USD
 * @param orphanPercent  approved orders that never get an ACK (the sweeper releases them)
 * @param requests       concurrency mode: number of parallel checks
 * @param maxInFlight    load mode: cap on outstanding requests before the client counts drops
 */
@ConfigurationProperties(prefix = "sim")
public record SimProperties(@DefaultValue("flow") String mode,
                            @DefaultValue("http://localhost:8080") String apiUrl,
                            String jwtSecret,
                            @DefaultValue("20") int rate,
                            @DefaultValue("60s") Duration duration,
                            @DefaultValue("100000") long minNotional,
                            @DefaultValue("2000000") long maxNotional,
                            @DefaultValue("1") int orphanPercent,
                            @DefaultValue("500") int requests,
                            @DefaultValue("2000") int maxInFlight) {
}
