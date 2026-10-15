package com.poc.credit.common;

/** Kafka topic names. */
public final class Topics {

    /** Acks, fills, cancels, rejects and amends from gateways. Keyed by groupId. */
    public static final String ORDER_EVENTS = "orders.events";

    /**
     * Every credit decision and every reservation state change, each carrying the full
     * reservation state and a version. The source of truth: Redis can be rebuilt from it.
     * Keyed by groupId. Downstream risk, surveillance and reporting consume it directly.
     */
    public static final String RESERVATIONS = "credit.reservations";

    /** Limit definitions and overrides. Compacted, keyed by LEVEL:id. */
    public static final String LIMITS = "credit.limits";

    private Topics() {
    }
}
