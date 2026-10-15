package com.poc.credit.common.redis;

import com.poc.credit.common.model.OrderEventType;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.List;

/**
 * Thin wrapper over the five Lua scripts that hold all of the credit correctness.
 * Each script runs atomically inside Redis, so no locks are needed in Java.
 * Spring sends EVALSHA and falls back to EVAL the first time a node has not cached a script.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class CreditScripts {

    private final StringRedisTemplate redis;
    private final RedisScript<List> reserve = load("lua/reserve.lua");
    private final RedisScript<List> amend = load("lua/amend.lua");
    private final RedisScript<List> applyEvent = load("lua/apply_event.lua");
    private final RedisScript<List> expire = load("lua/expire.lua");
    private final RedisScript<List> release = load("lua/release.lua");

    public CreditScripts(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** Atomic check-and-reserve at ACCOUNT and GROUP level. See reserve.lua. */
    public ScriptResult reserve(String groupId, String accountId, String orderId, long amountCents,
                                String requestHash, long nowMillis, Duration terminalTtl) {
        return run(reserve, Keys.scriptKeys(groupId, accountId, orderId),
                orderId, amountCents, requestHash, nowMillis, accountId, groupId, terminalTtl.toSeconds());
    }

    /** Pre-check an amend; reserves only an increase. See amend.lua. */
    public ScriptResult amend(String groupId, String accountId, String orderId, String amendId,
                              long newTotalCents, long nowMillis) {
        return run(amend, Keys.scriptKeys(groupId, accountId, orderId).subList(0, 3),
                amendId, newTotalCents, nowMillis);
    }

    /** Apply one order lifecycle event. See apply_event.lua. */
    public ScriptResult applyEvent(String groupId, String accountId, String orderId, OrderEventType type,
                                   long seq, long amountCents, long nowMillis, Duration terminalTtl) {
        return run(applyEvent, Keys.scriptKeys(groupId, accountId, orderId),
                type.name(), seq, amountCents, nowMillis, terminalTtl.toSeconds(), orderId);
    }

    /** Release an unacknowledged reservation created before the cutoff. See expire.lua. */
    public ScriptResult expire(String groupId, String accountId, String orderId, long cutoffMillis,
                               Duration expiredTtl) {
        return run(expire, Keys.scriptKeys(groupId, accountId, orderId),
                orderId, cutoffMillis, expiredTtl.toSeconds());
    }

    /** Undo a reservation whose RESERVED event could not be published. See release.lua. */
    public ScriptResult release(String groupId, String accountId, String orderId, String requestHash) {
        return run(release, Keys.scriptKeys(groupId, accountId, orderId), orderId, requestHash);
    }

    private ScriptResult run(RedisScript<List> script, List<String> keys, Object... args) {
        Object[] stringArgs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            stringArgs[i] = String.valueOf(args[i]);
        }
        List<Object> reply = redis.execute(script, keys, stringArgs);
        return ScriptResult.parse(reply);
    }

    private static RedisScript<List> load(String path) {
        return RedisScript.of(new ClassPathResource(path), List.class);
    }
}
