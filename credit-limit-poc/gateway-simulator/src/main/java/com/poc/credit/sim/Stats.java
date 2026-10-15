package com.poc.credit.sim;

import com.poc.credit.common.api.CreditCheckResponse;
import com.poc.credit.common.model.Decision;

import java.util.concurrent.atomic.LongAdder;

/** Counters and latency for a run. */
public class Stats {

    public final LatencyHistogram latency = new LatencyHistogram();
    public final LongAdder sent = new LongAdder();
    public final LongAdder approved = new LongAdder();
    public final LongAdder rejectedAccount = new LongAdder();
    public final LongAdder rejectedGroup = new LongAdder();
    public final LongAdder unavailable = new LongAdder();
    public final LongAdder errors = new LongAdder();
    public final LongAdder dropped = new LongAdder();

    public void record(CreditApiClient.Result<CreditCheckResponse> r) {
        latency.record(r.latencyMicros());
        if (r.ok()) {
            if (r.body().decision() == Decision.APPROVED) {
                approved.increment();
            } else if ("GROUP".equals(r.body().failedLevel())) {
                rejectedGroup.increment();
            } else {
                rejectedAccount.increment();
            }
        } else if (r.status() == 503) {
            unavailable.increment();
        } else {
            errors.increment();
        }
    }

    public String line() {
        return String.format("sent=%d approved=%d rejected[account=%d group=%d] 503=%d errors=%d | p50=%.2fms p99=%.2fms",
                sent.sum(), approved.sum(), rejectedAccount.sum(), rejectedGroup.sum(), unavailable.sum(), errors.sum(),
                latency.percentileMillis(50), latency.percentileMillis(99));
    }
}
