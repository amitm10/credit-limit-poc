package com.poc.credit.api.web;

import com.poc.credit.api.config.CreditProperties;
import com.poc.credit.common.security.DevTokens;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * DEV PROFILE ONLY: hands out tokens so the demo can use plain curl. In production this
 * endpoint does not exist; tokens come from the bank's identity provider.
 */
@Profile("dev")
@RestController
public class DevTokenController {

    private final CreditProperties props;

    public DevTokenController(CreditProperties props) {
        this.props = props;
    }

    @PostMapping("/dev/token")
    public Map<String, String> token(@RequestParam(defaultValue = DevTokens.ROLE_GATEWAY) String role,
                                     @RequestParam(defaultValue = "demo-user") String subject) {
        if (!List.of(DevTokens.ROLE_GATEWAY, DevTokens.ROLE_CREDIT_OFFICER).contains(role)) {
            throw new IllegalArgumentException("role must be GATEWAY or CREDIT_OFFICER");
        }
        return Map.of("token", DevTokens.mint(props.jwtSecret(), subject, List.of(role), Duration.ofHours(8)));
    }
}
