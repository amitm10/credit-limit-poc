package com.poc.credit.common.api;

import com.poc.credit.common.model.Decision;

import java.math.BigDecimal;

/**
 * A rejection is a valid business answer, so it is returned with HTTP 200.
 *
 * @param headroomUsd headroom after this decision (after the reservation when approved)
 * @param failedLevel ACCOUNT or GROUP when rejected, otherwise null
 */
public record CreditCheckResponse(Decision decision,
                                  String reservationId,
                                  BigDecimal exposureUsd,
                                  Headroom headroomUsd,
                                  String failedLevel) {

    public record Headroom(BigDecimal account, BigDecimal group) {
    }
}
