package com.poc.credit.processor.service;

import com.poc.credit.common.Json;
import com.poc.credit.common.Topics;
import com.poc.credit.common.events.LimitDefinition;
import com.poc.credit.common.events.ReservationEvent;
import com.poc.credit.common.model.LimitLevel;
import com.poc.credit.common.model.ReservationState;
import com.poc.credit.common.redis.Keys;
import com.poc.credit.processor.config.ProcessorProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Rebuilds all Redis state from Kafka, which is the source of truth.
 *
 * <ol>
 *   <li>Set the maintenance key: every API instance answers 503 (fail closed).</li>
 *   <li>Stop the order-event listener and the orphan sweeper, so nothing writes meanwhile.</li>
 *   <li>Read credit.limits (compacted) and credit.reservations to their current end.</li>
 *   <li>Keep the highest-version event per order: each event carries the full state.</li>
 *   <li>Wipe limits, reservations and mappings, then write them back with used and
 *       reserved summed from the orders.</li>
 *   <li>Restart the listener: uncommitted order events are re-applied, and the scripts'
 *       sequence and cumulative-fill checks make that idempotent.</li>
 * </ol>
 *
 * If anything fails, maintenance stays on and the listener stays stopped: better to reject
 * trades than to check them against state that might be wrong. Run the rebuild again.
 *
 * <p>POC limits: the latest state of every order in the retention window is held in memory,
 * and SCAN assumes a single primary. Production would read from periodic compacted
 * snapshots and rebuild shard by shard.
 */
@Service
public class RebuildService {

    private static final Logger log = LoggerFactory.getLogger(RebuildService.class);
    private static final int CHUNK = 1_000;
    private static final Duration READ_DEADLINE = Duration.ofMinutes(10);

    public record RebuildReport(boolean success, String trigger, long durationMs, int limitDefinitions,
                                long reservationEvents, int orders, int liveOrders, String error,
                                Instant completedAt) {
    }

    private final StringRedisTemplate redis;
    private final KafkaListenerEndpointRegistry listeners;
    private final OrphanSweeper sweeper;
    private final KafkaProperties kafkaProperties;
    private final ProcessorProperties props;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile RebuildReport lastReport;

    public RebuildService(StringRedisTemplate redis, KafkaListenerEndpointRegistry listeners, OrphanSweeper sweeper,
                          KafkaProperties kafkaProperties, ProcessorProperties props) {
        this.redis = redis;
        this.listeners = listeners;
        this.sweeper = sweeper;
        this.kafkaProperties = kafkaProperties;
        this.props = props;
    }

    public boolean isRunning() {
        return running.get();
    }

    public RebuildReport lastReport() {
        return lastReport;
    }

    public RebuildReport rebuild(String trigger) {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("A rebuild is already running");
        }
        long start = System.currentTimeMillis();
        MessageListenerContainer container = listeners.getListenerContainer(OrderEventListener.LISTENER_ID);
        log.warn("REBUILD started ({}): API entering maintenance mode", trigger);
        try {
            redis.opsForValue().set(Keys.MAINTENANCE, trigger);
            sweeper.pause();
            if (container != null) {
                container.stop();
            }
            Thread.sleep(500); // let checks already past the maintenance check finish publishing

            Map<String, LimitDefinition> limits = new LinkedHashMap<>();
            readTopic(Topics.LIMITS, r -> {
                if (r.value() != null) {
                    LimitDefinition d = Json.read(r.value(), LimitDefinition.class);
                    limits.put(d.key(), d);
                }
            });

            Map<String, ReservationEvent> orders = new HashMap<>();
            long events = readTopic(Topics.RESERVATIONS, r -> {
                if (r.value() == null) {
                    return;
                }
                ReservationEvent e = Json.read(r.value(), ReservationEvent.class);
                orders.merge(e.orderId(), e, (a, b) -> b.version() >= a.version() ? b : a);
            });

            wipe();
            writeLimits(limits.values());
            int live = writeReservations(orders.values(), limits);

            redis.delete(Keys.MAINTENANCE);
            if (container != null) {
                container.start();
            }
            sweeper.resume();
            lastReport = new RebuildReport(true, trigger, System.currentTimeMillis() - start, limits.size(), events,
                    orders.size(), live, null, Instant.now());
            log.warn("REBUILD complete in {} ms: {} limits, {} reservation events, {} orders ({} live). API accepting checks.",
                    lastReport.durationMs(), limits.size(), events, orders.size(), live);
            return lastReport;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("REBUILD FAILED: API stays in maintenance (fail closed) until a rebuild succeeds", e);
            lastReport = new RebuildReport(false, trigger, System.currentTimeMillis() - start, 0, 0, 0, 0,
                    e.getMessage(), Instant.now());
            return lastReport;
        } finally {
            running.set(false);
        }
    }

    /** Reads a topic from the beginning to the end offsets captured at the start. */
    private long readTopic(String topic, Consumer<ConsumerRecord<String, String>> handler) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", kafkaProperties.getBootstrapServers()));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5_000);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "rebuild-" + topic);
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10)).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions, Duration.ofSeconds(10));
            long deadline = System.currentTimeMillis() + READ_DEADLINE.toMillis();
            long count = 0;
            while (!caughtUp(consumer, end)) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("Timed out reading " + topic);
                }
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    handler.accept(r);
                    count++;
                }
            }
            log.info("Rebuild read {} records from {}", count, topic);
            return count;
        }
    }

    private static boolean caughtUp(KafkaConsumer<String, String> consumer, Map<TopicPartition, Long> end) {
        for (Map.Entry<TopicPartition, Long> e : end.entrySet()) {
            if (consumer.position(e.getKey()) < e.getValue()) {
                return false;
            }
        }
        return true;
    }

    private void wipe() {
        long n = deleteMatching("lim:*") + deleteMatching("rsv:*") + deleteMatching("acct:*");
        redis.delete(Keys.GROUPS_INDEX);
        log.info("Rebuild wiped {} keys", n);
    }

    private long deleteMatching(String pattern) {
        long deleted = 0;
        List<String> batch = new ArrayList<>(CHUNK);
        try (Cursor<String> cursor = redis.scan(ScanOptions.scanOptions().match(pattern).count(CHUNK).build())) {
            while (cursor.hasNext()) {
                batch.add(cursor.next());
                if (batch.size() == CHUNK) {
                    deleted += unlink(batch);
                }
            }
        }
        return deleted + unlink(batch);
    }

    private long unlink(List<String> batch) {
        if (batch.isEmpty()) {
            return 0;
        }
        Long n = redis.unlink(batch);
        batch.clear();
        return n == null ? 0 : n;
    }

    private void writeLimits(Iterable<LimitDefinition> definitions) {
        long now = System.currentTimeMillis();
        RedisSerializer<String> s = redis.getStringSerializer();
        List<LimitDefinition> chunk = new ArrayList<>(CHUNK);
        for (LimitDefinition d : definitions) {
            chunk.add(d);
            if (chunk.size() == CHUNK) {
                flushLimits(chunk, s, now);
            }
        }
        flushLimits(chunk, s, now);
    }

    private void flushLimits(List<LimitDefinition> chunk, RedisSerializer<String> s, long now) {
        if (chunk.isEmpty()) {
            return;
        }
        redis.executePipelined((RedisCallback<Object>) c -> {
            for (LimitDefinition d : chunk) {
                Map<byte[], byte[]> fields = new HashMap<>();
                fields.put(s.serialize("limit"), s.serialize(Long.toString(d.limitCents())));
                fields.put(s.serialize("used"), s.serialize("0"));
                fields.put(s.serialize("reserved"), s.serialize("0"));
                if (d.overrideLimitCents() != null && d.overrideExpiryMillis() != null && d.overrideExpiryMillis() > now) {
                    fields.put(s.serialize("overrideLimit"), s.serialize(Long.toString(d.overrideLimitCents())));
                    fields.put(s.serialize("overrideExpiry"), s.serialize(Long.toString(d.overrideExpiryMillis())));
                    fields.put(s.serialize("overrideReason"), s.serialize(String.valueOf(d.overrideReason())));
                    fields.put(s.serialize("overrideBy"), s.serialize(String.valueOf(d.updatedBy())));
                }
                if (d.level() == LimitLevel.GROUP) {
                    c.hashCommands().hMSet(s.serialize(Keys.groupLimit(d.id())), fields);
                    c.setCommands().sAdd(s.serialize(Keys.GROUPS_INDEX), s.serialize(d.id()));
                } else {
                    c.hashCommands().hMSet(s.serialize(Keys.accountLimit(d.groupId(), d.id())), fields);
                    c.stringCommands().set(s.serialize(Keys.accountGroup(d.id())), s.serialize(d.groupId()));
                }
            }
            return null;
        });
        chunk.clear();
    }

    /** Writes reservation hashes and sums used/reserved per account and group. Returns live orders. */
    private int writeReservations(Iterable<ReservationEvent> orders, Map<String, LimitDefinition> limits) {
        long now = System.currentTimeMillis();
        RedisSerializer<String> s = redis.getStringSerializer();
        Map<String, long[]> accountTotals = new HashMap<>();
        Map<String, long[]> groupTotals = new HashMap<>();
        List<ReservationEvent> chunk = new ArrayList<>(CHUNK);
        int live = 0;

        for (ReservationEvent e : orders) {
            if (ReservationEvent.SOURCE_COMPENSATION.equals(e.source())) {
                continue; // released and deleted by the API; the gateway was told 503
            }
            ReservationState state = ReservationState.valueOf(e.state());
            if (state != ReservationState.DECLINED) {
                long reserved = state.isLive() ? Math.max(e.amountCents() - e.filledCents(), 0) : 0;
                add(accountTotals, Keys.accountLimit(e.groupId(), e.accountId()), e.filledCents(), reserved);
                add(groupTotals, Keys.groupLimit(e.groupId()), e.filledCents(), reserved);
            }
            if (state.isLive()) {
                live++;
            }
            if (state.isLive() || e.ts() + ttlFor(state).toMillis() > now) {
                chunk.add(e);
            }
            if (chunk.size() == CHUNK) {
                flushReservations(chunk, s);
            }
        }
        flushReservations(chunk, s);

        int unmatched = 0;
        for (Map<String, long[]> totals : List.of(accountTotals, groupTotals)) {
            for (Map.Entry<String, long[]> t : totals.entrySet()) {
                if (Boolean.FALSE.equals(redis.hasKey(t.getKey()))) {
                    unmatched++;
                    continue;
                }
                redis.opsForHash().putAll(t.getKey(), Map.of(
                        "used", Long.toString(t.getValue()[0]),
                        "reserved", Long.toString(t.getValue()[1])));
            }
        }
        if (unmatched > 0) {
            log.warn("Rebuild found exposure for {} limit keys with no limit definition", unmatched);
        }
        return live;
    }

    private void flushReservations(List<ReservationEvent> chunk, RedisSerializer<String> s) {
        if (chunk.isEmpty()) {
            return;
        }
        redis.executePipelined((RedisCallback<Object>) c -> {
            for (ReservationEvent e : chunk) {
                ReservationState state = ReservationState.valueOf(e.state());
                byte[] key = s.serialize(Keys.reservation(e.groupId(), e.orderId()));
                Map<byte[], byte[]> fields = new HashMap<>();
                e.toHash().forEach((k, v) -> fields.put(s.serialize(k), s.serialize(v)));
                c.hashCommands().hMSet(key, fields);
                if (state == ReservationState.RESERVED) {
                    c.zSetCommands().zAdd(s.serialize(Keys.pending(e.groupId())), e.createdTs(), s.serialize(e.orderId()));
                }
                if (!state.isLive()) {
                    c.keyCommands().expire(key, ttlFor(state).toSeconds());
                }
            }
            return null;
        });
        chunk.clear();
    }

    private Duration ttlFor(ReservationState state) {
        return state == ReservationState.EXPIRED ? props.expiredTtl() : props.terminalTtl();
    }

    private static void add(Map<String, long[]> totals, String key, long used, long reserved) {
        long[] t = totals.computeIfAbsent(key, k -> new long[2]);
        t[0] += used;
        t[1] += reserved;
    }
}
