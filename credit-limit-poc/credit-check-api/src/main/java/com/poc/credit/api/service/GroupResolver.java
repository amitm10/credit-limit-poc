package com.poc.credit.api.service;

import com.poc.credit.common.redis.Keys;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves an account's parent group, which the caller never supplies. The mapping is
 * reference data that changes rarely, so positive lookups are cached in memory (POC
 * simplification: a group reassignment would need a restart or a cache TTL).
 */
@Component
public class GroupResolver {

    private final StringRedisTemplate redis;
    private final Map<String, String> cache = new ConcurrentHashMap<>();

    public GroupResolver(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public String groupOf(String accountId) {
        String cached = cache.get(accountId);
        if (cached != null) {
            return cached;
        }
        String group = redis.opsForValue().get(Keys.accountGroup(accountId));
        if (group == null) {
            throw new Errors.NotFoundException("Unknown account " + accountId);
        }
        cache.put(accountId, group);
        return group;
    }
}
