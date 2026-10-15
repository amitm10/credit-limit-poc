package com.poc.credit.processor.service;

import com.poc.credit.processor.config.ProcessorProperties;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sentinel replication is asynchronous, so a promoted replica can be missing the last
 * writes of the old primary. When Sentinel announces +switch-master, this triggers a
 * rebuild from Kafka, which restores anything the new primary lost.
 */
@Component
public class FailoverListener {

    private static final Logger log = LoggerFactory.getLogger(FailoverListener.class);

    private final RedisProperties redisProperties;
    private final ProcessorProperties props;
    private final RebuildService rebuild;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private RedisClient client;
    private StatefulRedisPubSubConnection<String, String> connection;

    public FailoverListener(RedisProperties redisProperties, ProcessorProperties props, RebuildService rebuild) {
        this.redisProperties = redisProperties;
        this.props = props;
        this.rebuild = rebuild;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void subscribe() {
        RedisProperties.Sentinel sentinel = redisProperties.getSentinel();
        if (!props.reconcileOnFailover() || sentinel == null || sentinel.getNodes() == null) {
            log.info("Sentinel failover reconciliation disabled");
            return;
        }
        client = RedisClient.create();
        client.setOptions(ClientOptions.builder().protocolVersion(ProtocolVersion.RESP2).autoReconnect(true).build());
        List<String> nodes = sentinel.getNodes();
        for (String node : nodes) {
            try {
                String[] hostPort = node.split(":");
                connection = client.connectPubSub(RedisURI.create(hostPort[0], Integer.parseInt(hostPort[1])));
                connection.addListener(new RedisPubSubAdapter<>() {
                    @Override
                    public void message(String channel, String message) {
                        onSwitchMaster(message);
                    }
                });
                connection.sync().subscribe("+switch-master");
                log.info("Listening for Sentinel +switch-master on {}", node);
                return;
            } catch (RuntimeException e) {
                log.warn("Could not subscribe to sentinel {}: {}", node, e.getMessage());
            }
        }
        log.error("No sentinel reachable; failover reconciliation is OFF");
    }

    private void onSwitchMaster(String message) {
        log.warn("Sentinel failover: {}. Rebuilding from Kafka in {} to recover any lost writes.",
                message, props.failoverRebuildDelay());
        executor.schedule(() -> {
            try {
                rebuild.rebuild("sentinel-failover");
            } catch (IllegalStateException alreadyRunning) {
                log.info("Rebuild already running");
            }
        }, props.failoverRebuildDelay().toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void close() {
        executor.shutdownNow();
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }
}
