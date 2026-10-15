package com.poc.credit.api.health;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Readiness includes Kafka: an instance that cannot publish decisions cannot approve
 * anything, so it should not be counted as ready.
 */
@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    private final AdminClient admin;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1500);
        this.admin = AdminClient.create(config);
    }

    @Override
    public Health health() {
        try {
            String clusterId = admin.describeCluster(new DescribeClusterOptions().timeoutMs(1000))
                    .clusterId().get(1500, TimeUnit.MILLISECONDS);
            return Health.up().withDetail("clusterId", clusterId).build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down(e).build();
        } catch (Exception e) {
            return Health.down(e).build();
        }
    }

    @PreDestroy
    void close() {
        admin.close();
    }
}
