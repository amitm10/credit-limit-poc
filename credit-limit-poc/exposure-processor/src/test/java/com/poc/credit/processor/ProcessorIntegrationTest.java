package com.poc.credit.processor;

import com.poc.credit.common.Json;
import com.poc.credit.common.Topics;
import com.poc.credit.common.events.LimitDefinition;
import com.poc.credit.common.events.OrderEvent;
import com.poc.credit.common.events.ReservationEvent;
import com.poc.credit.common.model.OrderEventType;
import com.poc.credit.common.redis.CreditScripts;
import com.poc.credit.common.redis.Keys;
import com.poc.credit.common.redis.ScriptResult;
import com.poc.credit.processor.service.RebuildService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Order lifecycle through Kafka into Redis, then a full rebuild of Redis from Kafka.
 * Real Redis via Testcontainers (needs Docker) and an embedded Kafka broker.
 * Each test uses its own group and account, because Kafka keeps every test's events.
 */
@SpringBootTest(properties = "credit.sweep-interval-ms=600000")
@EmbeddedKafka(partitions = 3, topics = {Topics.ORDER_EVENTS, Topics.RESERVATIONS, Topics.LIMITS},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class ProcessorIntegrationTest {

    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);
    static final Duration TTL = Duration.ofMinutes(10);
    static final Duration WAIT = Duration.ofSeconds(20);

    static {
        REDIS.start();
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    StringRedisTemplate redis;
    @Autowired
    CreditScripts scripts;
    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    RebuildService rebuild;

    /** What the limit loader does: limits in Redis and on the compacted credit.limits topic. */
    private void seed(String g, String a) throws Exception {
        redis.opsForHash().putAll(Keys.groupLimit(g), Map.of("limit", "100000000", "used", "0", "reserved", "0"));
        redis.opsForHash().putAll(Keys.accountLimit(g, a), Map.of("limit", "10000000", "used", "0", "reserved", "0"));
        redis.opsForValue().set(Keys.accountGroup(a), g);
        redis.opsForSet().add(Keys.GROUPS_INDEX, g);
        kafka.send(Topics.LIMITS, "GROUP:" + g, Json.write(LimitDefinition.group(g, 100_000_000L, "test"))).get();
        kafka.send(Topics.LIMITS, "ACCOUNT:" + a, Json.write(LimitDefinition.account(a, g, 10_000_000L, "test"))).get();
    }

    /** What the API does on an approved check: reserve in Redis, publish RESERVED. */
    private void approve(String g, String a, String orderId, long cents) throws Exception {
        ScriptResult r = scripts.reserve(g, a, orderId, cents, "h-" + orderId, System.currentTimeMillis(), TTL);
        assertThat(r.status()).isEqualTo("APPROVED");
        kafka.send(Topics.RESERVATIONS, g, Json.write(ReservationEvent.fromHash(r.reservation(), "CHECK", ""))).get();
    }

    /** Gateway event. FILL carries the cumulative quantity at price 1.00, so $1 per unit. */
    private void send(String g, String a, String orderId, OrderEventType type, long seq, long cumQty) throws Exception {
        OrderEvent e = new OrderEvent(orderId, a, g, type, seq, cumQty, BigDecimal.ONE, System.currentTimeMillis());
        kafka.send(Topics.ORDER_EVENTS, g, Json.write(e)).get();
    }

    private long[] exposure(String g, String a) {
        List<Object> v = redis.opsForHash().multiGet(Keys.accountLimit(g, a), List.of("used", "reserved"));
        return new long[]{Long.parseLong((String) v.get(0)), Long.parseLong((String) v.get(1))};
    }

    private Object state(String g, String orderId) {
        return redis.opsForHash().get(Keys.reservation(g, orderId), "state");
    }

    @Test
    void appliesAckFillsAndCancelIdempotently() throws Exception {
        String g = "G-LIFE";
        String a = "ACC-LIFE";
        seed(g, a);
        approve(g, a, "P1", 1_000_000);                  // $10,000 reserved
        send(g, a, "P1", OrderEventType.ACK, 1, 0);
        send(g, a, "P1", OrderEventType.FILL, 2, 3_000); // cumulative $3,000 filled
        send(g, a, "P1", OrderEventType.FILL, 2, 3_000); // duplicate delivery
        send(g, a, "P1", OrderEventType.CANCEL, 3, 0);

        await().atMost(WAIT).untilAsserted(() -> assertThat(state(g, "P1")).isEqualTo("CANCELLED"));
        assertThat(exposure(g, a)).containsExactly(300_000, 0);
    }

    @Test
    void rebuildRestoresExposureFromKafkaAlone() throws Exception {
        String g = "G-REBUILD";
        String a = "ACC-REBUILD";
        seed(g, a);
        approve(g, a, "R1", 2_000_000);                  // $20,000, ACKed, $5,000 filled
        send(g, a, "R1", OrderEventType.ACK, 1, 0);
        send(g, a, "R1", OrderEventType.FILL, 2, 5_000);
        approve(g, a, "R2", 1_500_000);                  // $15,000, fully filled
        send(g, a, "R2", OrderEventType.FILL, 1, 15_000);
        approve(g, a, "R3", 700_000);                    // $7,000, still awaiting ACK

        await().atMost(WAIT).untilAsserted(() -> {
            assertThat(state(g, "R2")).isEqualTo("FILLED");
            assertThat(state(g, "R1")).isEqualTo("PARTIALLY_FILLED");
        });
        long[] before = exposure(g, a);
        // used = 5,000 + 15,000 = $20,000; reserved = 15,000 + 7,000 = $22,000 (in cents)
        assertThat(before).containsExactly(2_000_000, 2_200_000);

        redis.execute((RedisCallback<Void>) c -> {
            c.serverCommands().flushAll();
            return null;
        });
        RebuildService.RebuildReport report = rebuild.rebuild("test");

        assertThat(report.success()).isTrue();
        assertThat(exposure(g, a)).containsExactly(before);
        assertThat(state(g, "R1")).isEqualTo("PARTIALLY_FILLED");
        assertThat(redis.opsForZSet().score(Keys.pending(g), "R3")).isNotNull();
        assertThat(redis.hasKey(Keys.MAINTENANCE)).isFalse();

        // The listener is running again and applies new events on top of the rebuilt state.
        send(g, a, "R1", OrderEventType.CANCEL, 3, 0);
        await().atMost(WAIT).untilAsserted(() -> assertThat(exposure(g, a)).containsExactly(2_000_000, 700_000));
    }
}
