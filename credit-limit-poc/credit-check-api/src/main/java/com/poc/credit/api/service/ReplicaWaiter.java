package com.poc.credit.api.service;

import com.poc.credit.api.config.CreditProperties;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis replication is asynchronous, so a primary can acknowledge a reservation and die
 * before its replica has it. WAIT narrows that window: it returns once N replicas confirm
 * this connection's writes, or after the timeout. A timeout is counted and logged but does
 * not fail the check; after a failover the processor replays Kafka to recover anything lost.
 * Must be called inside the same Redis session as the write (see CreditCheckService).
 *
 * <p>WAIT replies with an integer. Spring's generic {@code connection.execute("WAIT")} decodes
 * unknown commands as bytes and fails on an integer reply, so this dispatches the command
 * through Lettuce with an integer output instead.
 */
@Component
public class ReplicaWaiter {

    private static final Logger log = LoggerFactory.getLogger(ReplicaWaiter.class);

    private static final ProtocolKeyword WAIT = new ProtocolKeyword() {
        private final byte[] bytes = "WAIT".getBytes(StandardCharsets.US_ASCII);

        @Override
        public byte[] getBytes() {
            return bytes;
        }

        public String name() {
            return "WAIT";
        }
    };

    private final StringRedisTemplate redis;
    private final CreditProperties props;
    private final Counter timeouts;
    private final Counter failures;
    private final AtomicBoolean traceLogged = new AtomicBoolean();

    public ReplicaWaiter(StringRedisTemplate redis, CreditProperties props, MeterRegistry registry) {
        this.redis = redis;
        this.props = props;
        this.timeouts = Counter.builder("credit.redis.wait.timeouts")
                .description("Reservations not confirmed by a replica within the WAIT timeout")
                .register(registry);
        this.failures = Counter.builder("credit.redis.wait.failures")
                .description("WAIT calls that threw instead of returning a replica count")
                .register(registry);
    }

    public void awaitReplication() {
        int replicas = props.waitReplicas();
        if (replicas <= 0) {
            return;
        }
        long timeoutMs = props.waitTimeout().toMillis();
        try {
            Long acked = redis.execute((RedisCallback<Long>) connection ->
                    dispatchWait(connection.getNativeConnection(), replicas, timeoutMs));
            if (acked == null || acked < replicas) {
                timeouts.increment();
                log.debug("WAIT timed out: {} of {} replicas confirmed", acked, replicas);
            }
        } catch (RuntimeException e) {
            // The reservation is already made. Losing the replica confirmation must not fail the
            // check (the Kafka publish that follows is the durability guarantee), but it is counted
            // and the first failure is logged with its full cause.
            failures.increment();
            timeouts.increment();
            if (traceLogged.compareAndSet(false, true)) {
                log.warn("WAIT failed, continuing without replica confirmation (stack trace logged once)", e);
            } else {
                log.debug("WAIT failed: {}", e.toString());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Long dispatchWait(Object nativeConnection, int replicas, long timeoutMs) {
        if (!(nativeConnection instanceof BaseRedisAsyncCommands<?, ?> raw)) {
            throw new IllegalStateException("Unsupported native Redis connection: "
                    + (nativeConnection == null ? "null" : nativeConnection.getClass().getName()));
        }
        BaseRedisAsyncCommands<byte[], byte[]> commands = (BaseRedisAsyncCommands<byte[], byte[]>) raw;
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE)
                .add((long) replicas)
                .add(timeoutMs);
        RedisFuture<Long> reply = commands.dispatch(WAIT, new IntegerOutput<>(ByteArrayCodec.INSTANCE), args);
        try {
            return reply.get(timeoutMs + 1_000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for replicas", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("WAIT did not complete", e);
        }
    }
}
