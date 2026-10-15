package com.poc.credit.common;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * All exposure is held as whole US cents in a {@code long}, so Redis can use integer
 * HINCRBY and nothing ever drifts through floating point. $500M is 5e10 cents, far inside
 * both the long range and the 2^53 exact-integer range of Lua numbers.
 */
public final class Money {

    private Money() {
    }

    /** Notional in cents = quantity x price, rounded half-up to the cent. */
    public static long notionalCents(long quantity, BigDecimal price) {
        return price.multiply(BigDecimal.valueOf(quantity))
                .movePointRight(2)
                .setScale(0, RoundingMode.HALF_UP)
                .longValueExact();
    }

    public static long toCents(BigDecimal usd) {
        return usd.movePointRight(2).setScale(0, RoundingMode.HALF_UP).longValueExact();
    }

    public static BigDecimal toUsd(long cents) {
        return BigDecimal.valueOf(cents, 2);
    }

    public static BigDecimal toUsd(Long cents) {
        return cents == null ? null : toUsd(cents.longValue());
    }
}
