package com.poc.credit.common.api;

import com.poc.credit.common.model.Decision;

import java.math.BigDecimal;

/**
 * @param direction   UP (delta reserved now), DOWN (released when the venue confirms) or NONE
 * @param failedLevel ACCOUNT or GROUP when rejected, otherwise null
 */
public record AmendResponse(Decision decision,
                            String orderId,
                            String direction,
                            BigDecimal orderExposureUsd,
                            CreditCheckResponse.Headroom headroomUsd,
                            String failedLevel) {
}
