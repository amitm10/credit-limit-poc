package com.poc.credit.api;

import com.poc.credit.common.api.CreditCheckResponse;
import com.poc.credit.common.model.Decision;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The test that justifies Redis: 500 parallel HTTP checks against one account, with total
 * demand at twice its headroom. Exactly half fit, and reserved exposure never exceeds the limit.
 */
class ConcurrencyTest extends ApiTestSupport {

    @Test
    void fiveHundredParallelChecksNeverOverCommit() throws Exception {
        flushRedis();
        String group = "G-CONC-TEST";
        String account = "ACC-CONC-TEST";
        seedAccount(group, account, 1_000_000_000_00L, 10_000_000_00L); // account headroom $10M

        // 500 orders x 400 @ $100 = $40,000 each = $20M of demand against $10M headroom.
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<ResponseEntity<CreditCheckResponse>>> calls = IntStream.range(0, 500)
                    .mapToObj(i -> pool.submit(() -> http.postForEntity("/api/v1/credit-checks",
                            json(checkBody("CONC-" + i, account, 400, "100"), "GATEWAY"), CreditCheckResponse.class)))
                    .toList();
            long approved = 0;
            long rejected = 0;
            for (Future<ResponseEntity<CreditCheckResponse>> call : calls) {
                ResponseEntity<CreditCheckResponse> res = call.get();
                assertThat(res.getStatusCode().is2xxSuccessful()).isTrue();
                if (res.getBody().decision() == Decision.APPROVED) {
                    approved++;
                } else {
                    rejected++;
                }
            }
            assertThat(approved).isEqualTo(250);
            assertThat(rejected).isEqualTo(250);
            assertThat(reservedCents(group, account)).isEqualTo(250 * 40_000_00L).isLessThanOrEqualTo(10_000_000_00L);
        }
    }
}
