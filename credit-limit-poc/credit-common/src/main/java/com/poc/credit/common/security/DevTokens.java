package com.poc.credit.common.security;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;

/**
 * POC only: mints HS256 tokens with a shared secret. In production the bank's identity
 * provider issues the tokens and the API validates them against its JWKS endpoint; the API's
 * authorization rules do not change.
 */
public final class DevTokens {

    public static final String ISSUER = "credit-poc-dev";
    public static final String ROLES_CLAIM = "roles";
    public static final String ROLE_GATEWAY = "GATEWAY";
    public static final String ROLE_CREDIT_OFFICER = "CREDIT_OFFICER";

    private DevTokens() {
    }

    public static String mint(String secret, String subject, List<String> roles, Duration ttl) {
        Instant now = Instant.now();
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .issuer(ISSUER)
                .subject(subject)
                .claim(ROLES_CLAIM, roles)
                .issueTime(Date.from(now))
                .expirationTime(Date.from(now.plus(ttl)))
                .build();
        SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
        try {
            jwt.sign(new MACSigner(secret.getBytes(StandardCharsets.UTF_8)));
        } catch (JOSEException e) {
            throw new IllegalStateException("Cannot sign token (secret must be at least 32 bytes)", e);
        }
        return jwt.serialize();
    }
}
