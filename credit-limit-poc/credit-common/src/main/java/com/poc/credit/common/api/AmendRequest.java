package com.poc.credit.common.api;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/**
 * Body of {@code POST /api/v1/credit-checks/{orderId}/amend}.
 *
 * @param amendId   idempotency key for this amend
 * @param accountId the order's account (locates the reservation under its group)
 */
public record AmendRequest(
        @NotBlank @Pattern(regexp = "[A-Za-z0-9._:-]{1,64}") String amendId,
        @NotBlank @Pattern(regexp = "[A-Za-z0-9._:-]{1,64}") String accountId,
        @Positive long newQuantity,
        @NotNull @Positive @Digits(integer = 12, fraction = 6) BigDecimal newPrice) {
}
