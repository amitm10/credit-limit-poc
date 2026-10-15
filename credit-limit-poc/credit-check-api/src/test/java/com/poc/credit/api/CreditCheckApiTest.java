package com.poc.credit.api;

import com.poc.credit.common.Json;
import com.poc.credit.common.Topics;
import com.poc.credit.common.api.CreditCheckResponse;
import com.poc.credit.common.events.ReservationEvent;
import com.poc.credit.common.model.Decision;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CreditCheckApiTest extends ApiTestSupport {

    static final String G = "G-API";
    static final String A = "ACC-API";

    @Autowired
    EmbeddedKafkaBroker broker;

    @BeforeEach
    void seed() {
        flushRedis();
        seedAccount(G, A, 100_000_000_00L, 1_000_000_00L); // group $100M, account $1M
    }

    private ResponseEntity<CreditCheckResponse> check(String orderId, long qty, String price) {
        return http.postForEntity("/api/v1/credit-checks", json(checkBody(orderId, A, qty, price), "GATEWAY"),
                CreditCheckResponse.class);
    }

    @Test
    void approvesReservesAndPublishesTheDecision() {
        String orderId = "ORD-" + UUID.randomUUID();
        ResponseEntity<CreditCheckResponse> res = check(orderId, 10_000, "54.20"); // $542,000

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody().decision()).isEqualTo(Decision.APPROVED);
        assertThat(res.getBody().exposureUsd()).isEqualByComparingTo("542000.00");
        assertThat(res.getBody().headroomUsd().account()).isEqualByComparingTo("458000.00");
        assertThat(reservedCents(G, A)).isEqualTo(542_000_00L);

        List<ReservationEvent> events = eventsFor(orderId);
        assertThat(events).anySatisfy(e -> {
            assertThat(e.state()).isEqualTo("RESERVED");
            assertThat(e.amountCents()).isEqualTo(542_000_00L);
            assertThat(e.version()).isEqualTo(1);
        });
    }

    @Test
    void rejectsAtAccountLevelWith200AndNamesTheLevel() {
        ResponseEntity<CreditCheckResponse> res = check("ORD-BIG-" + UUID.randomUUID(), 20_000, "54.20");
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody().decision()).isEqualTo(Decision.REJECTED);
        assertThat(res.getBody().failedLevel()).isEqualTo("ACCOUNT");
        assertThat(reservedCents(G, A)).isZero();
    }

    @Test
    void retryReplaysAndReusedOrderIdConflicts() {
        String orderId = "ORD-" + UUID.randomUUID();
        check(orderId, 1_000, "100");
        ResponseEntity<CreditCheckResponse> retry = check(orderId, 1_000, "100.00");
        assertThat(retry.getBody().decision()).isEqualTo(Decision.APPROVED);
        assertThat(reservedCents(G, A)).isEqualTo(100_000_00L);

        ResponseEntity<String> reused = http.postForEntity("/api/v1/credit-checks",
                json(checkBody(orderId, A, 2_000, "100"), "GATEWAY"), String.class);
        assertThat(reused.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void validationAndSecurity() {
        assertThat(http.postForEntity("/api/v1/credit-checks",
                json(checkBody("ORD-X", "ACC-UNKNOWN", 1, "1"), "GATEWAY"), String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(http.postForEntity("/api/v1/credit-checks",
                json(checkBody("ORD-X", A, -5, "1"), "GATEWAY"), String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.postForEntity("/api/v1/credit-checks",
                new HttpEntity<>(checkBody("ORD-X", A, 1, "1")), String.class).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(http.postForEntity("/api/v1/credit-checks",
                json(checkBody("ORD-X", A, 1, "1"), "CREDIT_OFFICER"), String.class).getStatusCode())
                .isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void creditOfficerSeesExposureAtBothLevels() {
        check("ORD-" + UUID.randomUUID(), 1_000, "100");
        ResponseEntity<Map> res = http.exchange("/api/v1/exposure/" + A, org.springframework.http.HttpMethod.GET,
                new HttpEntity<>(auth("CREDIT_OFFICER")), Map.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(res.getBody()).containsKeys("account", "group");
    }

    private List<ReservationEvent> eventsFor(String orderId) {
        Map<String, Object> props = KafkaTestUtils.consumerProps("test-" + UUID.randomUUID(), "false", broker);
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            // Read the whole topic from the beginning: assign every partition explicitly.
            List<TopicPartition> partitions = consumer.partitionsFor(Topics.RESERVATIONS).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            List<ReservationEvent> found = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (found.isEmpty() && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    ReservationEvent e = Json.read(r.value(), ReservationEvent.class);
                    if (orderId.equals(e.orderId())) {
                        found.add(e);
                    }
                }
            }
            return found;
        }
    }
}
