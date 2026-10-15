package com.poc.credit.common.events;

import java.util.Map;

/**
 * Published to {@code credit.reservations} for every credit decision and every reservation
 * state change. It carries the whole reservation state plus a monotonically increasing
 * version, so the latest event per order is enough to rebuild Redis.
 *
 * @param source CHECK, AMEND, ORDER_EVENT, SWEEPER or COMPENSATION
 * @param alert  empty, or LATE_ACK, LATE_FILL, OVERFILL, UNCHECKED_INCREASE
 */
public record ReservationEvent(String orderId,
                               String accountId,
                               String groupId,
                               String state,
                               long amountCents,
                               long filledCents,
                               long pendingAmendCents,
                               long lastEventSeq,
                               long version,
                               String failedLevel,
                               String requestHash,
                               long createdTs,
                               String source,
                               String alert,
                               long ts) {

    public static final String SOURCE_CHECK = "CHECK";
    public static final String SOURCE_AMEND = "AMEND";
    public static final String SOURCE_ORDER_EVENT = "ORDER_EVENT";
    public static final String SOURCE_SWEEPER = "SWEEPER";
    public static final String SOURCE_COMPENSATION = "COMPENSATION";

    /** Builds the event from the reservation hash returned by a Lua script. */
    public static ReservationEvent fromHash(Map<String, String> h, String source, String alert) {
        return new ReservationEvent(
                h.get("orderId"), h.get("accountId"), h.get("groupId"), h.get("state"),
                num(h, "amountCents"), num(h, "filledCents"), num(h, "pendingAmendCents"),
                num(h, "lastEventSeq"), num(h, "version"),
                h.getOrDefault("failedLevel", ""), h.getOrDefault("requestHash", ""),
                num(h, "createdTs"), source, alert == null ? "" : alert, System.currentTimeMillis());
    }

    /** Converts back to reservation hash fields, used by the rebuild. */
    public Map<String, String> toHash() {
        return Map.ofEntries(
                Map.entry("orderId", orderId), Map.entry("accountId", accountId), Map.entry("groupId", groupId),
                Map.entry("state", state), Map.entry("failedLevel", failedLevel == null ? "" : failedLevel),
                Map.entry("amountCents", Long.toString(amountCents)),
                Map.entry("filledCents", Long.toString(filledCents)),
                Map.entry("pendingAmendCents", Long.toString(pendingAmendCents)),
                Map.entry("lastEventSeq", Long.toString(lastEventSeq)),
                Map.entry("version", Long.toString(version)),
                Map.entry("requestHash", requestHash == null ? "" : requestHash),
                Map.entry("createdTs", Long.toString(createdTs)));
    }

    private static long num(Map<String, String> h, String k) {
        String v = h.get(k);
        return v == null || v.isEmpty() ? 0L : Long.parseLong(v);
    }
}
