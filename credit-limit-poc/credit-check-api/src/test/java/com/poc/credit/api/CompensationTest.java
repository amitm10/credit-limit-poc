package com.poc.credit.api;

import com.poc.credit.api.service.EventPublisher;
import com.poc.credit.api.service.PublishFailedException;
import com.poc.credit.common.events.ReservationEvent;
import com.poc.credit.common.redis.Keys;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

/** Kafka refuses the RESERVED event: the API must answer 503 and leave no reservation behind. */
class CompensationTest extends ApiTestSupport {

    @MockitoBean
    EventPublisher publisher;

    @Test
    void publishFailureReleasesTheReservationAndAnswers503() {
        flushRedis();
        seedAccount("G-COMP", "ACC-COMP", 100_000_000_00L, 1_000_000_00L);
        doThrow(new PublishFailedException("broker down", null)).when(publisher).publish(any(ReservationEvent.class));

        ResponseEntity<String> res = http.postForEntity("/api/v1/credit-checks",
                json(checkBody("ORD-COMP-1", "ACC-COMP", 1_000, "100"), "GATEWAY"), String.class);

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(res.getHeaders().getFirst("Retry-After")).isEqualTo("1");
        assertThat(reservedCents("G-COMP", "ACC-COMP")).isZero();
        assertThat(redis.hasKey(Keys.reservation("G-COMP", "ORD-COMP-1"))).isFalse();

        // Kafka is back: the gateway's retry gets a fresh, successful check.
        reset(publisher);
        doNothing().when(publisher).publish(any(ReservationEvent.class));
        ResponseEntity<String> retry = http.postForEntity("/api/v1/credit-checks",
                json(checkBody("ORD-COMP-1", "ACC-COMP", 1_000, "100"), "GATEWAY"), String.class);
        assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(reservedCents("G-COMP", "ACC-COMP")).isEqualTo(100_000_00L);
    }
}
