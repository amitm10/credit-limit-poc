package com.poc.credit.common.events;

import com.poc.credit.common.model.LimitLevel;

/**
 * A limit as approved by the credit risk system, published to the compacted
 * {@code credit.limits} topic keyed by {@link #key()}. Overrides are published here too,
 * which gives every override an audit trail (who, why, until when).
 */
public record LimitDefinition(LimitLevel level,
                              String id,
                              String groupId,
                              long limitCents,
                              Long overrideLimitCents,
                              Long overrideExpiryMillis,
                              String overrideReason,
                              String updatedBy,
                              long ts) {

    public String key() {
        return level + ":" + id;
    }

    public static LimitDefinition account(String accountId, String groupId, long limitCents, String updatedBy) {
        return new LimitDefinition(LimitLevel.ACCOUNT, accountId, groupId, limitCents,
                null, null, null, updatedBy, System.currentTimeMillis());
    }

    public static LimitDefinition group(String groupId, long limitCents, String updatedBy) {
        return new LimitDefinition(LimitLevel.GROUP, groupId, groupId, limitCents,
                null, null, null, updatedBy, System.currentTimeMillis());
    }

    public LimitDefinition withOverride(long overrideCents, long expiryMillis, String reason, String by) {
        return new LimitDefinition(level, id, groupId, limitCents, overrideCents, expiryMillis, reason, by,
                System.currentTimeMillis());
    }
}
