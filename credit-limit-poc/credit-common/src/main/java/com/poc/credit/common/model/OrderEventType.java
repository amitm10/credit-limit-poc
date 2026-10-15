package com.poc.credit.common.model;

/** Order lifecycle events published by gateways to {@code orders.events}. */
public enum OrderEventType {
    ACK,
    /** quantity = cumulative filled quantity, price = average fill price (like FIX CumQty/AvgPx). */
    FILL,
    CANCEL,
    REJECT,
    /** quantity and price = the new total order size confirmed by the venue. */
    AMEND_CONFIRMED,
    AMEND_REJECTED
}
