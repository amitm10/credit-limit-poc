package com.poc.credit.common.api;

import java.math.BigDecimal;
import java.time.Instant;

/** Response of {@code GET /api/v1/exposure/{accountId}}: both levels of the hierarchy. */
public record ExposureView(String accountId, String groupId, Level account, Level group) {

    /**
     * @param effectiveLimitUsd the override when one is active, otherwise the approved limit
     * @param headroomUsd       effective limit minus used minus reserved
     */
    public record Level(BigDecimal limitUsd,
                        BigDecimal effectiveLimitUsd,
                        BigDecimal usedUsd,
                        BigDecimal reservedUsd,
                        BigDecimal headroomUsd,
                        BigDecimal overrideLimitUsd,
                        Instant overrideExpiresAt,
                        String overrideReason) {
    }
}
