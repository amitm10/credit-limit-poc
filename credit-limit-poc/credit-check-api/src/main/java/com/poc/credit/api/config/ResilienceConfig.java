package com.poc.credit.api.config;

import com.poc.credit.api.service.PublishFailedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;

import java.time.Duration;

@Configuration
public class ResilienceConfig {

    /**
     * Opens after 50% of calls fail within a 10 second window (minimum 20 calls), then
     * rejects immediately with 503 for 5 seconds instead of letting threads pile up on a
     * dead dependency. Only infrastructure failures count; business outcomes such as a
     * rejection, an unknown account or a conflict never trip it.
     */
    @Bean
    CircuitBreaker checkCircuitBreaker(MeterRegistry meterRegistry) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(20)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .permittedNumberOfCallsInHalfOpenState(5)
                .recordException(e -> e instanceof DataAccessException || e instanceof PublishFailedException)
                .build();
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meterRegistry);
        return registry.circuitBreaker("credit-check");
    }
}
