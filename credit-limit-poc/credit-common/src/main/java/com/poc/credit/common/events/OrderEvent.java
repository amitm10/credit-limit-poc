package com.poc.credit.common.events;

import com.poc.credit.common.Money;
import com.poc.credit.common.model.OrderEventType;

import java.math.BigDecimal;

/**
 * An order lifecycle event from a gateway, published to {@code orders.events} keyed by groupId.
 *
 * @param seq      per-order sequence number, increasing in the order the venue produced events
 * @param quantity FILL: cumulative filled quantity. AMEND_CONFIRMED: new total quantity. Else unused.
 * @param price    FILL: average fill price. AMEND_CONFIRMED: new price. Else unused.
 */
public record OrderEvent(String orderId,
                         String accountId,
                         String groupId,
                         OrderEventType type,
                         long seq,
                         long quantity,
                         BigDecimal price,
                         long ts) {

    /** Cumulative filled notional for FILL, new total notional for AMEND_CONFIRMED, else 0. */
    public long amountCents() {
        if ((type == OrderEventType.FILL || type == OrderEventType.AMEND_CONFIRMED) && price != null) {
            return Money.notionalCents(quantity, price);
        }
        return 0L;
    }

    public static OrderEvent simple(String orderId, String accountId, String groupId, OrderEventType type, long seq) {
        return new OrderEvent(orderId, accountId, groupId, type, seq, 0L, null, System.currentTimeMillis());
    }
}
