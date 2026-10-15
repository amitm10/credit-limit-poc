package com.poc.credit.api;

import com.poc.credit.common.Topics;
import com.poc.credit.common.redis.Keys;
import com.poc.credit.common.security.DevTokens;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Real Redis (Testcontainers, needs Docker) and an embedded Kafka broker. One Redis
 * container is shared by all API tests.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "credit.jwt-secret=" + ApiTestSupport.SECRET)
@EmbeddedKafka(partitions = 3, topics = {Topics.RESERVATIONS, Topics.LIMITS, Topics.ORDER_EVENTS},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public abstract class ApiTestSupport {

    static final String SECRET = "test-secret-that-is-at-least-32-bytes-long";
    static final GenericContainer<?> REDIS;

    static {
        REDIS = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
        REDIS.start();
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    protected TestRestTemplate http;

    @Autowired
    protected StringRedisTemplate redis;

    protected void flushRedis() {
        redis.execute((RedisCallback<Void>) c -> {
            c.serverCommands().flushAll();
            return null;
        });
    }

    /** Creates an account and its group with the given limits, in cents. */
    protected void seedAccount(String groupId, String accountId, long groupLimitCents, long accountLimitCents) {
        redis.opsForHash().putAll(Keys.groupLimit(groupId),
                Map.of("limit", Long.toString(groupLimitCents), "used", "0", "reserved", "0"));
        redis.opsForHash().putAll(Keys.accountLimit(groupId, accountId),
                Map.of("limit", Long.toString(accountLimitCents), "used", "0", "reserved", "0"));
        redis.opsForValue().set(Keys.accountGroup(accountId), groupId);
    }

    protected long reservedCents(String groupId, String accountId) {
        Object v = redis.opsForHash().get(Keys.accountLimit(groupId, accountId), "reserved");
        return v == null ? 0 : Long.parseLong((String) v);
    }

    protected static HttpHeaders auth(String role) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(DevTokens.mint(SECRET, "test-" + role.toLowerCase(), List.of(role), Duration.ofMinutes(5)));
        return headers;
    }

    protected static HttpEntity<String> json(String body, String role) {
        return new HttpEntity<>(body, auth(role));
    }

    protected static String checkBody(String orderId, String accountId, long quantity, String price) {
        return """
                {"orderId":"%s","accountId":"%s","side":"BUY","quantity":%d,"price":%s}
                """.formatted(orderId, accountId, quantity, price);
    }
}
