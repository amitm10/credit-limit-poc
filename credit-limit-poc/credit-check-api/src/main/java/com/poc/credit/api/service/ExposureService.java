package com.poc.credit.api.service;

import com.poc.credit.common.Money;
import com.poc.credit.common.api.ExposureView;
import com.poc.credit.common.api.LimitOverrideRequest;
import com.poc.credit.common.events.LimitDefinition;
import com.poc.credit.common.redis.Keys;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Credit officer views and temporary limit overrides. */
@Service
public class ExposureService {

    private static final List<Object> FIELDS = List.of(
            "limit", "used", "reserved", "overrideLimit", "overrideExpiry", "overrideReason");

    private final StringRedisTemplate redis;
    private final GroupResolver groups;
    private final EventPublisher publisher;

    public ExposureService(StringRedisTemplate redis, GroupResolver groups, EventPublisher publisher) {
        this.redis = redis;
        this.groups = groups;
        this.publisher = publisher;
    }

    public ExposureView view(String accountId) {
        String groupId = groups.groupOf(accountId);
        return new ExposureView(accountId, groupId,
                level(Keys.accountLimit(groupId, accountId)), level(Keys.groupLimit(groupId)));
    }

    /**
     * Kafka first, then Redis: the audit event is the source of truth, so an override that
     * reached Kafka survives a rebuild even if the Redis write fails and the officer retries.
     */
    public ExposureView override(String accountId, LimitOverrideRequest req, String officer) {
        String groupId = groups.groupOf(accountId);
        String key = Keys.accountLimit(groupId, accountId);
        Object limit = redis.opsForHash().get(key, "limit");
        if (limit == null) {
            throw new Errors.NotFoundException("No limit loaded for account " + accountId);
        }
        long overrideCents = Money.toCents(req.limitUsd());
        long expiry = req.expiresAt().toEpochMilli();

        LimitDefinition definition = LimitDefinition.account(accountId, groupId, Long.parseLong((String) limit), officer)
                .withOverride(overrideCents, expiry, req.reason(), officer);
        publisher.publish(definition);

        redis.opsForHash().putAll(key, Map.of(
                "overrideLimit", Long.toString(overrideCents),
                "overrideExpiry", Long.toString(expiry),
                "overrideReason", req.reason(),
                "overrideBy", officer));
        return view(accountId);
    }

    public ReservationView reservation(String orderId, String accountId) {
        String groupId = groups.groupOf(accountId);
        Map<Object, Object> h = redis.opsForHash().entries(Keys.reservation(groupId, orderId));
        if (h.isEmpty() || !accountId.equals(h.get("accountId"))) {
            throw new Errors.NotFoundException("No reservation for order " + orderId + " on account " + accountId);
        }
        String failedLevel = (String) h.get("failedLevel");
        return new ReservationView(orderId, accountId, groupId, (String) h.get("state"),
                Money.toUsd(parse(h.get("amountCents"))), Money.toUsd(parse(h.get("filledCents"))),
                Money.toUsd(parse(h.get("pendingAmendCents"))), parse(h.get("lastEventSeq")),
                parse(h.get("version")), failedLevel == null || failedLevel.isEmpty() ? null : failedLevel,
                Instant.ofEpochMilli(parse(h.get("createdTs"))));
    }

    public record ReservationView(String orderId, String accountId, String groupId, String state,
                                  BigDecimal amountUsd, BigDecimal filledUsd,
                                  BigDecimal pendingAmendUsd, long lastEventSeq, long version,
                                  String failedLevel, Instant createdAt) {
    }

    private ExposureView.Level level(String key) {
        List<Object> v = redis.opsForHash().multiGet(key, FIELDS);
        if (v.get(0) == null) {
            throw new Errors.NotFoundException("No limit loaded at " + key);
        }
        long limit = parse(v.get(0));
        long used = parse(v.get(1));
        long reserved = parse(v.get(2));
        Long overrideLimit = v.get(3) == null ? null : parse(v.get(3));
        Long overrideExpiry = v.get(4) == null ? null : parse(v.get(4));
        boolean active = overrideLimit != null && overrideExpiry != null && overrideExpiry > System.currentTimeMillis();
        long effective = active ? overrideLimit : limit;
        return new ExposureView.Level(
                Money.toUsd(limit), Money.toUsd(effective), Money.toUsd(used), Money.toUsd(reserved),
                Money.toUsd(effective - used - reserved),
                active ? Money.toUsd(overrideLimit) : null,
                active ? Instant.ofEpochMilli(overrideExpiry) : null,
                active ? (String) v.get(5) : null);
    }

    private static long parse(Object o) {
        return o == null ? 0L : Long.parseLong((String) o);
    }
}
