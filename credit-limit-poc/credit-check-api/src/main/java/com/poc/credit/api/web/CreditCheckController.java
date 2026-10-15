package com.poc.credit.api.web;

import com.poc.credit.api.service.AmendService;
import com.poc.credit.api.service.CreditCheckService;
import com.poc.credit.api.service.ExposureService;
import com.poc.credit.common.api.AmendRequest;
import com.poc.credit.common.api.AmendResponse;
import com.poc.credit.common.api.CreditCheckRequest;
import com.poc.credit.common.api.CreditCheckResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class CreditCheckController {

    private final CreditCheckService checks;
    private final AmendService amends;
    private final ExposureService exposure;

    public CreditCheckController(CreditCheckService checks, AmendService amends, ExposureService exposure) {
        this.checks = checks;
        this.amends = amends;
        this.exposure = exposure;
    }

    /** Check and reserve for a new order. 200 for both APPROVED and REJECTED. */
    @PostMapping("/credit-checks")
    public CreditCheckResponse check(@Valid @RequestBody CreditCheckRequest request) {
        return checks.check(request);
    }

    /** Re-check an amend; reserves only an increase. */
    @PostMapping("/credit-checks/{orderId}/amend")
    public AmendResponse amend(@PathVariable String orderId, @Valid @RequestBody AmendRequest request) {
        return amends.amend(orderId, request);
    }

    @GetMapping("/reservations/{orderId}")
    public ExposureService.ReservationView reservation(@PathVariable String orderId,
                                                       @RequestParam String accountId) {
        return exposure.reservation(orderId, accountId);
    }
}
