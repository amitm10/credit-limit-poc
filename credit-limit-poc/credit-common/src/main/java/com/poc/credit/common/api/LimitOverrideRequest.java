package com.poc.credit.common.api;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;

/** Body of {@code PUT /api/v1/limits/{accountId}/override}: a temporary account limit. */
public record LimitOverrideRequest(
        @NotNull @Positive @Digits(integer = 12, fraction = 2) BigDecimal limitUsd,
        @NotNull @Future Instant expiresAt,
        @NotBlank @Size(max = 200) String reason) {
}
