package com.poc.credit.common.api;

import com.poc.credit.common.model.Side;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/**
 * Body of {@code POST /api/v1/credit-checks}. The group, the limits and the exposure are
 * never taken from the caller; the service looks them up itself.
 *
 * @param orderId unique order ID, also the idempotency key
 */
public record CreditCheckRequest(
        @NotBlank @Pattern(regexp = "[A-Za-z0-9._:-]{1,64}") String orderId,
        @NotBlank @Pattern(regexp = "[A-Za-z0-9._:-]{1,64}") String accountId,
        @NotNull Side side,
        @Positive long quantity,
        @NotNull @Positive @Digits(integer = 12, fraction = 6) BigDecimal price) {
}
