package com.poc.credit.processor.config;

import com.poc.credit.common.redis.CreditScripts;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class ProcessorConfig {

    @Bean
    CreditScripts creditScripts(StringRedisTemplate redis) {
        return new CreditScripts(redis);
    }

    /**
     * An order event must never be dropped: if Redis or Kafka is unavailable, retry the same
     * record every second until it succeeds. Offsets are committed only after processing,
     * and the scripts are idempotent, so retries and redeliveries are safe. Unparseable
     * records are handled (counted and skipped) inside the listener, never here.
     */
    @Bean
    CommonErrorHandler retryForever() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
