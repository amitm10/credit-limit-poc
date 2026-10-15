package com.poc.credit.api.web;

import com.poc.credit.api.service.ExposureService;
import com.poc.credit.common.api.ExposureView;
import com.poc.credit.common.api.LimitOverrideRequest;
import jakarta.validation.Valid;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Credit officer endpoints. */
@RestController
@RequestMapping("/api/v1")
public class ExposureController {

    private final ExposureService exposure;

    public ExposureController(ExposureService exposure) {
        this.exposure = exposure;
    }

    @GetMapping("/exposure/{accountId}")
    public ExposureView exposure(@PathVariable String accountId) {
        return exposure.view(accountId);
    }

    /** Temporary limit override; audited on the credit.limits topic with who, why and until when. */
    @PutMapping("/limits/{accountId}/override")
    public ExposureView override(@PathVariable String accountId,
                                 @Valid @RequestBody LimitOverrideRequest request,
                                 @AuthenticationPrincipal Jwt officer) {
        return exposure.override(accountId, request, officer.getSubject());
    }
}
