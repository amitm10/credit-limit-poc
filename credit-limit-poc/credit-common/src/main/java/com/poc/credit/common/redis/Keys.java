package com.poc.credit.common.redis;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Redis key scheme. Every key that one Lua script touches carries the same {groupId} hash
 * tag, so all of a group's keys land in one Redis Cluster slot and multi-key scripts stay
 * legal when the store is sharded.
 */
public final class Keys {

    /** IDs are embedded in keys, so braces and whitespace must never get through. */
    public static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9._:-]{1,64}");

    /** Set of every group ID; used by the orphan sweeper to find pending sets. */
    public static final String GROUPS_INDEX = "idx:groups";

    /** While this key exists, the API answers 503 (set during a rebuild). */
    public static final String MAINTENANCE = "credit:maintenance";

    private Keys() {
    }

    public static String groupLimit(String groupId) {
        return "lim:{" + safe(groupId) + "}:grp";
    }

    public static String accountLimit(String groupId, String accountId) {
        return "lim:{" + safe(groupId) + "}:acct:" + safe(accountId);
    }

    public static String reservation(String groupId, String orderId) {
        if ("pending".equals(orderId)) {
            throw new IllegalArgumentException("Reserved order ID: " + orderId);
        }
        return "rsv:{" + safe(groupId) + "}:" + safe(orderId);
    }

    public static String pending(String groupId) {
        return "rsv:{" + safe(groupId) + "}:pending";
    }

    /** Account to parent-group mapping, read before a script to build its keys. */
    public static String accountGroup(String accountId) {
        return "acct:{" + safe(accountId) + "}:grp";
    }

    /** KEYS order shared by reserve, apply_event, expire and release. */
    public static List<String> scriptKeys(String groupId, String accountId, String orderId) {
        return List.of(groupLimit(groupId), accountLimit(groupId, accountId),
                reservation(groupId, orderId), pending(groupId));
    }

    public static String safe(String id) {
        if (id == null || !SAFE_ID.matcher(id).matches()) {
            throw new IllegalArgumentException("Invalid identifier: " + id);
        }
        return id;
    }
}
