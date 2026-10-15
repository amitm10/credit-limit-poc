package com.poc.credit.sim;

import com.poc.credit.common.api.CreditCheckRequest;
import com.poc.credit.common.api.CreditCheckResponse;
import com.poc.credit.common.api.ExposureView;
import com.poc.credit.common.model.Decision;
import com.poc.credit.common.model.OrderEventType;
import com.poc.credit.common.model.Side;
import com.poc.credit.common.seed.SeedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@Component
public class Simulator {

    private static final Logger log = LoggerFactory.getLogger(Simulator.class);

    private final SimProperties props;
    private final KafkaTemplate<String, String> kafka;
    private final CreditApiClient api;
    private final AccountPicker accounts = new AccountPicker();
    private final String runId = Long.toString(System.currentTimeMillis(), 36).toUpperCase();
    private final AtomicLong sequence = new AtomicLong();

    public Simulator(SimProperties props, KafkaTemplate<String, String> kafka) {
        this.props = props;
        this.kafka = kafka;
        this.api = new CreditApiClient(props.apiUrl(), props.jwtSecret());
    }

    public int run() throws Exception {
        log.info("Gateway simulator: mode={} api={}", props.mode(), props.apiUrl());
        return switch (props.mode()) {
            case "flow" -> flow();
            case "load" -> load();
            case "concurrency" -> concurrency();
            default -> throw new IllegalArgumentException("sim.mode must be flow, load or concurrency");
        };
    }

    /** Continuous demo traffic until stopped. */
    private int flow() throws Exception {
        Stats stats = new Stats();
        OrderLifecycle lifecycle = new OrderLifecycle(kafka, api, props.orphanPercent(), 150);
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        long intervalNanos = 1_000_000_000L / Math.max(1, props.rate());
        long next = System.nanoTime();
        long lastPrint = System.currentTimeMillis();
        while (!Thread.currentThread().isInterrupted()) {
            next += intervalNanos;
            LockSupport.parkNanos(next - System.nanoTime());
            workers.submit(() -> placeOrder(stats, lifecycle));
            if (System.currentTimeMillis() - lastPrint >= 5_000) {
                lastPrint = System.currentTimeMillis();
                log.info("{} | events={} orphans={} amends={}", stats.line(), lifecycle.events(),
                        lifecycle.orphans(), lifecycle.amends());
            }
        }
        return 0;
    }

    /** Open-model load: a fixed arrival rate for a fixed duration, whatever the latency. */
    private int load() throws Exception {
        Stats stats = new Stats();
        OrderLifecycle lifecycle = new OrderLifecycle(kafka, api, props.orphanPercent(), 20);
        Semaphore inFlight = new Semaphore(props.maxInFlight());
        long total = (long) props.rate() * props.duration().toSeconds();
        long intervalNanos = 1_000_000_000L / Math.max(1, props.rate());
        log.info("Load: {} checks/s for {} s ({} checks), notional ${}-${}", props.rate(), props.duration().toSeconds(),
                total, props.minNotional(), props.maxNotional());

        long startNanos = System.nanoTime();
        long lastPrint = System.currentTimeMillis();
        try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (long i = 0; i < total; i++) {
                LockSupport.parkNanos(startNanos + i * intervalNanos - System.nanoTime());
                if (!inFlight.tryAcquire()) {
                    stats.dropped.increment(); // the client itself is saturated; reported, never hidden
                    continue;
                }
                workers.submit(() -> {
                    try {
                        placeOrder(stats, lifecycle);
                    } finally {
                        inFlight.release();
                    }
                });
                if (System.currentTimeMillis() - lastPrint >= 5_000) {
                    lastPrint = System.currentTimeMillis();
                    log.info(stats.line());
                }
            }
        }
        double seconds = (System.nanoTime() - startNanos) / 1e9;
        lifecycle.drain(2_000);

        LatencyHistogram h = stats.latency;
        String report = String.join("\n",
                "",
                "================ LOAD TEST RESULT ================",
                String.format("Target rate        %,d checks/s for %d s", props.rate(), props.duration().toSeconds()),
                String.format("Achieved           %,.0f checks/s (%,d completed in %.1f s)", h.count() / seconds, h.count(), seconds),
                String.format("Approved           %,d", stats.approved.sum()),
                String.format("Rejected           %,d account level, %,d group level", stats.rejectedAccount.sum(), stats.rejectedGroup.sum()),
                String.format("503 (fail closed)  %,d", stats.unavailable.sum()),
                String.format("Other errors       %,d", stats.errors.sum()),
                String.format("Client drops       %,d (client saturated at %d in flight)", stats.dropped.sum(), props.maxInFlight()),
                String.format("Latency (client)   p50 %.2f ms | p90 %.2f ms | p99 %.2f ms | p99.9 %.2f ms | max %.2f ms",
                        h.percentileMillis(50), h.percentileMillis(90), h.percentileMillis(99), h.percentileMillis(99.9), h.maxMillis()),
                String.format("Lifecycle events   %,d published to orders.events", lifecycle.events()),
                "Note: client-side latency includes nginx and the HTTP client, all on one laptop.",
                "==================================================");
        log.info(report);
        return 0;
    }

    /**
     * The race: N parallel checks against one account, total demand = 2x its headroom,
     * released together through a start gate. Passes if approved exposure never exceeds the
     * headroom and matches what Redis reserved exactly. Cancels the approved orders afterwards
     * so the test can be re-run.
     */
    private int concurrency() throws Exception {
        String account = SeedData.CONC_ACCOUNT;
        String group = SeedData.CONC_GROUP;
        CreditApiClient.Result<ExposureView> before = api.exposure(account);
        if (!before.ok()) {
            log.error("Cannot read exposure for {}: {} {}", account, before.status(), before.error());
            return 2;
        }
        BigDecimal headroom = before.body().account().headroomUsd();
        BigDecimal reservedBefore = before.body().account().reservedUsd();
        int n = props.requests();
        BigDecimal each = headroom.multiply(BigDecimal.valueOf(2))
                .divide(BigDecimal.valueOf(n), 2, RoundingMode.DOWN);
        if (each.signum() <= 0) {
            log.error("No headroom left on {}; wait for the previous run's cancels to apply", account);
            return 2;
        }

        CountDownLatch gate = new CountDownLatch(1);
        AtomicLong retried = new AtomicLong();
        List<Future<CreditApiClient.Result<CreditCheckResponse>>> calls = new ArrayList<>(n);
        List<String> orderIds = new ArrayList<>(n);
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n; i++) {
                String orderId = "CONC-" + runId + "-" + i;
                orderIds.add(orderId);
                CreditCheckRequest req = new CreditCheckRequest(orderId, account, Side.BUY, 1, each);
                calls.add(pool.submit(() -> {
                    gate.await();
                    return checkWithRetry(req, retried);
                }));
            }
            gate.countDown();

            long approved = 0;
            long rejected = 0;
            long failed = 0;
            List<String> approvedIds = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                CreditApiClient.Result<CreditCheckResponse> r = calls.get(i).get();
                if (r.ok() && r.body().decision() == Decision.APPROVED) {
                    approved++;
                    approvedIds.add(orderIds.get(i));
                } else if (r.ok()) {
                    rejected++;
                } else {
                    failed++;
                }
            }
            BigDecimal approvedSum = each.multiply(BigDecimal.valueOf(approved));
            CreditApiClient.Result<ExposureView> after = api.exposure(account);
            if (!after.ok()) {
                log.error("Cannot read exposure after the test: {} {}", after.status(), after.error());
                return 2;
            }
            BigDecimal reservedDelta = after.body().account().reservedUsd().subtract(reservedBefore);
            boolean noOverCommit = approvedSum.compareTo(headroom) <= 0;
            boolean consistent = approvedSum.compareTo(reservedDelta) == 0;
            long expected = headroom.divide(each, 0, RoundingMode.DOWN).longValue();

            String report = String.join("\n",
                    "",
                    "=============== CONCURRENCY TEST RESULT ===============",
                    String.format("Account              %s (group %s)", account, group),
                    String.format("Headroom before      $%,.2f", headroom),
                    String.format("Requests             %d in parallel, $%,.2f each", n, each),
                    String.format("Total demand         $%,.2f (2x headroom)", each.multiply(BigDecimal.valueOf(n))),
                    String.format("Approved             %d (maximum that fits: %d)", approved, expected),
                    String.format("Rejected             %d", rejected),
                    String.format("Errors               %d (after retries)", failed),
                    String.format("Retried after 503    %d (same order ID, so a retry can never reserve twice)", retried.get()),
                    String.format("Approved exposure    $%,.2f", approvedSum),
                    String.format("Reserved in Redis    $%,.2f (change during the test)", reservedDelta),
                    String.format("Over-commit          $%,.2f", approvedSum.subtract(headroom).max(BigDecimal.ZERO)),
                    "RESULT               " + (noOverCommit && consistent && failed == 0
                            ? "PASS: never over-committed, Redis matches the approvals exactly"
                            : "FAIL"),
                    "=======================================================");
            log.info(report);

            OrderLifecycle lifecycle = new OrderLifecycle(kafka, api, 0, 1);
            for (String id : approvedIds) {
                lifecycle.publish(new OrderLifecycle.Order(id, account, group, 1, each), OrderEventType.CANCEL, 1, 0);
            }
            kafka.flush();
            log.info("Published CANCEL for the {} approved orders; headroom is restored once they are applied.",
                    approvedIds.size());
            return noOverCommit && consistent && failed == 0 ? 0 : 1;
        }
    }

    /**
     * What a real gateway does with a 503: wait briefly and ask again with the same order ID.
     * The order ID is the idempotency key, so the service replays the first answer (or finishes a
     * reservation whose publish was interrupted) instead of reserving a second time.
     */
    private CreditApiClient.Result<CreditCheckResponse> checkWithRetry(CreditCheckRequest req, AtomicLong retried)
            throws InterruptedException {
        CreditApiClient.Result<CreditCheckResponse> r = api.check(req);
        for (int attempt = 1; attempt <= 4 && !r.ok() && retryable(r.status()); attempt++) {
            retried.incrementAndGet();
            Thread.sleep(250L * attempt);
            r = api.check(req);
        }
        return r;
    }

    private static boolean retryable(int status) {
        return status == 503 || status == 502 || status == 504 || status < 0;
    }

    private void placeOrder(Stats stats, OrderLifecycle lifecycle) {
        SeedData.Account account = accounts.next();
        ThreadLocalRandom r = ThreadLocalRandom.current();
        BigDecimal price = BigDecimal.valueOf(1_000 + r.nextInt(19_001), 2); // $10.00 - $200.00
        double logMin = Math.log(props.minNotional());
        double logMax = Math.log(Math.max(props.minNotional() + 1, props.maxNotional()));
        double notional = Math.exp(logMin + r.nextDouble() * (logMax - logMin));
        long quantity = Math.max(1, (long) (notional / price.doubleValue()));
        String orderId = "SIM-" + runId + "-" + sequence.incrementAndGet();

        stats.sent.increment();
        CreditApiClient.Result<CreditCheckResponse> res =
                api.check(new CreditCheckRequest(orderId, account.accountId(), r.nextBoolean() ? Side.BUY : Side.SELL,
                        quantity, price));
        stats.record(res);
        if (res.ok() && res.body().decision() == Decision.APPROVED) {
            lifecycle.start(new OrderLifecycle.Order(orderId, account.accountId(), account.groupId(), quantity, price));
        } else if (!res.ok() && stats.errors.sum() + stats.unavailable.sum() <= 5) {
            log.warn("Check failed: HTTP {} {}", res.status(), res.error());
        }
    }
}
