package com.poc.credit.common.model;

/** Lifecycle of a credit reservation. The names match the values the Lua scripts write. */
public enum ReservationState {
    /** Approved by the check; waiting for the venue to acknowledge the order. */
    RESERVED,
    /** Acknowledged by the venue and resting in the market. */
    WORKING,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED,
    /** Rejected by the venue after a successful credit check. */
    REJECTED,
    /** Never acknowledged within the orphan timeout; exposure released by the sweeper. */
    EXPIRED,
    /** Rejected by the credit check itself; nothing was reserved. */
    DECLINED;

    public boolean isLive() {
        return this == RESERVED || this == WORKING || this == PARTIALLY_FILLED;
    }

    public boolean isTerminal() {
        return this == FILLED || this == CANCELLED || this == REJECTED;
    }
}
