package com.poc.credit.sim;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/** Lock-free latency histogram: 10 microsecond buckets up to 1 second, plus an overflow bucket. */
public class LatencyHistogram {

    private static final int BUCKET_MICROS = 10;
    private static final int BUCKETS = 100_000;

    private final AtomicLongArray counts = new AtomicLongArray(BUCKETS + 1);
    private final AtomicLong total = new AtomicLong();
    private final AtomicLong max = new AtomicLong();

    public void record(long micros) {
        int bucket = (int) Math.min(micros / BUCKET_MICROS, BUCKETS);
        counts.incrementAndGet(bucket);
        total.incrementAndGet();
        max.accumulateAndGet(micros, Math::max);
    }

    public long count() {
        return total.get();
    }

    /** Upper bound of the bucket holding the given percentile, in milliseconds. */
    public double percentileMillis(double percentile) {
        long n = total.get();
        if (n == 0) {
            return 0;
        }
        long rank = (long) Math.ceil(percentile / 100.0 * n);
        long seen = 0;
        for (int i = 0; i <= BUCKETS; i++) {
            seen += counts.get(i);
            if (seen >= rank) {
                return i == BUCKETS ? maxMillis() : (i + 1) * BUCKET_MICROS / 1000.0;
            }
        }
        return maxMillis();
    }

    public double maxMillis() {
        return max.get() / 1000.0;
    }
}
