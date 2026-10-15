package com.poc.credit.loader;

import com.poc.credit.common.Json;
import com.poc.credit.common.Topics;
import com.poc.credit.common.events.LimitDefinition;
import com.poc.credit.common.model.LimitLevel;
import com.poc.credit.common.redis.Keys;
import com.poc.credit.common.seed.SeedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Stands in for the credit risk system's limit feed. Writes every limit to the compacted
 * credit.limits topic (the source of truth used by a rebuild) and to Redis, then exits.
 * Skips if Redis is already seeded, unless --credit.loader.force=true.
 */
@SpringBootApplication
public class LimitLoaderApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(LimitLoaderApplication.class);
    private static final int CHUNK = 1_000;

    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, String> kafka;
    private final boolean force;

    public LimitLoaderApplication(StringRedisTemplate redis, KafkaTemplate<String, String> kafka,
                                  @Value("${credit.loader.force:false}") boolean force) {
        this.redis = redis;
        this.kafka = kafka;
        this.force = force;
    }

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(LimitLoaderApplication.class, args)));
    }

    @Override
    public void run(String... args) throws Exception {
        if (!force && Boolean.TRUE.equals(redis.hasKey(Keys.GROUPS_INDEX))) {
            log.info("Limits already loaded ({} groups). Use --credit.loader.force=true to reload.",
                    redis.opsForSet().size(Keys.GROUPS_INDEX));
            return;
        }
        long start = System.currentTimeMillis();
        List<LimitDefinition> definitions = new ArrayList<>();
        SeedData.groups().forEach(g -> definitions.add(LimitDefinition.group(g.groupId(), g.limitCents(), "limit-loader")));
        SeedData.accounts().forEach(a -> definitions.add(
                LimitDefinition.account(a.accountId(), a.groupId(), a.limitCents(), "limit-loader")));

        // Kafka first: it is the source of truth.
        List<CompletableFuture<?>> sends = new ArrayList<>();
        for (LimitDefinition d : definitions) {
            sends.add(kafka.send(Topics.LIMITS, d.key(), Json.write(d)));
        }
        CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);

        RedisSerializer<String> s = redis.getStringSerializer();
        for (int i = 0; i < definitions.size(); i += CHUNK) {
            List<LimitDefinition> chunk = definitions.subList(i, Math.min(i + CHUNK, definitions.size()));
            redis.executePipelined((RedisCallback<Object>) c -> {
                for (LimitDefinition d : chunk) {
                    byte[] key = s.serialize(d.level() == LimitLevel.GROUP
                            ? Keys.groupLimit(d.id()) : Keys.accountLimit(d.groupId(), d.id()));
                    // Set the limit; never reset exposure that already exists (safe to re-run with force).
                    c.hashCommands().hSet(key, s.serialize("limit"), s.serialize(Long.toString(d.limitCents())));
                    c.hashCommands().hSetNX(key, s.serialize("used"), s.serialize("0"));
                    c.hashCommands().hSetNX(key, s.serialize("reserved"), s.serialize("0"));
                    if (d.level() == LimitLevel.GROUP) {
                        c.setCommands().sAdd(s.serialize(Keys.GROUPS_INDEX), s.serialize(d.id()));
                    } else {
                        c.stringCommands().set(s.serialize(Keys.accountGroup(d.id())), s.serialize(d.groupId()));
                    }
                }
                return null;
            });
        }
        log.info("Loaded {} groups and {} accounts into Kafka and Redis in {} ms",
                SeedData.groups().size(), SeedData.accounts().size(), System.currentTimeMillis() - start);
    }
}
