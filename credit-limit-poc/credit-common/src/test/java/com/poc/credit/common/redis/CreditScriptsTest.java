package com.poc.credit.common.redis;

import com.poc.credit.common.model.OrderEventType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the Lua scripts against a real Redis. Requires Docker. */
@Testcontainers
class CreditScriptsTest {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

    static LettuceConnectionFactory factory;
    static StringRedisTemplate redis;
    static CreditScripts scripts;
    static final Duration TTL = Duration.ofMinutes(10);
    static final String G = "G1";
    static final String A = "A1";

    @BeforeAll
    static void connect() {
        factory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379)));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
        scripts = new CreditScripts(redis);
    }

    @AfterAll
    static void close() {
        factory.destroy();
    }

    @BeforeEach
    void seed() {
        redis.execute((RedisCallback<Void>) c -> {
            c.serverCommands().flushAll();
            return null;
        });
        redis.opsForHash().putAll(Keys.groupLimit(G), Map.of("limit", "500000", "used", "0", "reserved", "0"));
        redis.opsForHash().putAll(Keys.accountLimit(G, A), Map.of("limit", "100000", "used", "0", "reserved", "0"));
    }

    private ScriptResult reserve(String order, long cents) {
        return scripts.reserve(G, A, order, cents, "h-" + order + "-" + cents, System.currentTimeMillis(), TTL);
    }

    private ScriptResult event(String order, OrderEventType type, long seq, long cents) {
        return scripts.applyEvent(G, A, order, type, seq, cents, System.currentTimeMillis(), TTL);
    }

    private long[] account() {
        List<Object> v = redis.opsForHash().multiGet(Keys.accountLimit(G, A), List.of("used", "reserved"));
        return new long[]{Long.parseLong((String) v.get(0)), Long.parseLong((String) v.get(1))};
    }

    @Test
    void approvesAndRejectsAtEachLevel() {
        ScriptResult ok = reserve("o1", 60_000);
        assertThat(ok.status()).isEqualTo("APPROVED");
        assertThat(ok.accountHeadroomCents()).isEqualTo(40_000L);
        assertThat(ok.groupHeadroomCents()).isEqualTo(440_000L);

        ScriptResult acct = reserve("o2", 50_000);
        assertThat(acct.status()).isEqualTo("DECLINED");
        assertThat(acct.detail()).isEqualTo("ACCOUNT");

        // A second account under the same group: its own limit is fine, the group's is not.
        redis.opsForHash().put(Keys.groupLimit(G), "limit", "100000");
        redis.opsForHash().putAll(Keys.accountLimit(G, "A2"), Map.of("limit", "900000", "used", "0", "reserved", "0"));
        ScriptResult grp = scripts.reserve(G, "A2", "o3", 50_000, "h3", System.currentTimeMillis(), TTL);
        assertThat(grp.status()).isEqualTo("DECLINED");
        assertThat(grp.detail()).isEqualTo("GROUP");
    }

    @Test
    void retriesReplayAndReusedOrderIdsConflict() {
        reserve("o1", 60_000);
        assertThat(reserve("o1", 60_000).status()).isEqualTo("REPLAY");
        assertThat(account()[1]).isEqualTo(60_000);
        assertThat(scripts.reserve(G, A, "o1", 70_000, "other", System.currentTimeMillis(), TTL).status())
                .isEqualTo("CONFLICT");
    }

    @Test
    void lifecycleWithCumulativeFillsNeverDoubleCounts() {
        reserve("o1", 10_000);
        event("o1", OrderEventType.ACK, 1, 0);
        event("o1", OrderEventType.FILL, 2, 3_000);
        assertThat(event("o1", OrderEventType.FILL, 2, 3_000).status()).isEqualTo("DUPLICATE");
        event("o1", OrderEventType.FILL, 4, 5_000);
        assertThat(event("o1", OrderEventType.FILL, 3, 4_000).detail()).isEqualTo("STALE_FILL");
        assertThat(account()).containsExactly(5_000, 5_000);

        ScriptResult cancel = event("o1", OrderEventType.CANCEL, 5, 0);
        assertThat(cancel.field("state")).isEqualTo("CANCELLED");
        assertThat(account()).containsExactly(5_000, 0);
    }

    @Test
    void orphanExpiresAndLateAckRevivesIt() {
        long old = System.currentTimeMillis() - 60_000;
        scripts.reserve(G, A, "o1", 10_000, "h", old, TTL);
        ScriptResult expired = scripts.expire(G, A, "o1", System.currentTimeMillis() - 30_000, Duration.ofDays(1));
        assertThat(expired.status()).isEqualTo("EXPIRED");
        assertThat(account()).containsExactly(0, 0);

        ScriptResult ack = event("o1", OrderEventType.ACK, 1, 0);
        assertThat(ack.detail()).isEqualTo("LATE_ACK");
        assertThat(account()).containsExactly(0, 10_000);
        assertThat(scripts.expire(G, A, "o1", System.currentTimeMillis(), Duration.ofDays(1)).status())
                .isEqualTo("SKIPPED");
    }

    @Test
    void amendUpReservesDeltaAndVenueRejectReleasesIt() {
        reserve("o1", 10_000);
        ScriptResult up = scripts.amend(G, A, "o1", "am1", 15_000, System.currentTimeMillis());
        assertThat(up.status()).isEqualTo("APPROVED");
        assertThat(up.detail()).isEqualTo("UP");
        assertThat(account()[1]).isEqualTo(15_000);
        event("o1", OrderEventType.AMEND_REJECTED, 1, 0);
        assertThat(account()[1]).isEqualTo(10_000);
    }

    @Test
    void compensationReleasesAndDeletes() {
        scripts.reserve(G, A, "o1", 10_000, "h1", System.currentTimeMillis(), TTL);
        ScriptResult released = scripts.release(G, A, "o1", "h1");
        assertThat(released.status()).isEqualTo("RELEASED");
        assertThat(released.field("version")).isEqualTo("2");
        assertThat(redis.hasKey(Keys.reservation(G, "o1"))).isFalse();
        assertThat(account()[1]).isZero();
    }

    @Test
    void fiveHundredConcurrentChecksNeverOverCommit() throws Exception {
        // 100,000 cents of headroom; 500 checks of 400 cents = 2x the headroom.
        try (ExecutorService pool = Executors.newFixedThreadPool(64)) {
            List<Future<ScriptResult>> results = IntStream.range(0, 500)
                    .mapToObj(i -> pool.submit(() -> reserve("c" + i, 400)))
                    .toList();
            long approved = 0;
            for (Future<ScriptResult> f : results) {
                if (f.get().is("APPROVED")) {
                    approved++;
                }
            }
            assertThat(approved).isEqualTo(250);
            assertThat(account()[1]).isEqualTo(100_000);
        }
    }
}
