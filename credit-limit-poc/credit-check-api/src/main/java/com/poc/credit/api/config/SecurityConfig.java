package com.poc.credit.api.config;

import com.poc.credit.common.security.DevTokens;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;

import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

/**
 * Stateless JWT resource server. Roles come from the token's "roles" claim:
 * GATEWAY may check and amend; CREDIT_OFFICER may view exposure and set overrides.
 */
@Configuration
public class SecurityConfig {

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health/**", "/actuator/info", "/actuator/prometheus").permitAll()
                        .requestMatchers("/dev/**", "/error").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/v1/credit-checks", "/api/v1/credit-checks/*/amend")
                        .hasRole(DevTokens.ROLE_GATEWAY)
                        .requestMatchers(HttpMethod.GET, "/api/v1/reservations/*")
                        .hasAnyRole(DevTokens.ROLE_GATEWAY, DevTokens.ROLE_CREDIT_OFFICER)
                        .requestMatchers("/api/v1/exposure/**", "/api/v1/limits/**")
                        .hasRole(DevTokens.ROLE_CREDIT_OFFICER)
                        .anyRequest().denyAll())
                .oauth2ResourceServer(oauth -> oauth.jwt(jwt -> jwt.jwtAuthenticationConverter(rolesConverter())));
        return http.build();
    }

    @Bean
    JwtDecoder jwtDecoder(CreditProperties props) {
        SecretKeySpec key = new SecretKeySpec(props.jwtSecret().getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withSecretKey(key).macAlgorithm(MacAlgorithm.HS256).build();
        decoder.setJwtValidator(JwtValidators.createDefaultWithIssuer(DevTokens.ISSUER));
        return decoder;
    }

    private static JwtAuthenticationConverter rolesConverter() {
        JwtGrantedAuthoritiesConverter roles = new JwtGrantedAuthoritiesConverter();
        roles.setAuthoritiesClaimName(DevTokens.ROLES_CLAIM);
        roles.setAuthorityPrefix("ROLE_");
        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(roles);
        return converter;
    }
}
