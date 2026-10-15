package com.poc.credit.sim;

import com.poc.credit.common.seed.SeedData;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Skewed traffic, because real flow concentrates on big clients:
 * 10% to the "mega fund" group G0000, 72% to the busiest 20% of the other groups, 18% to the rest.
 * The concurrency-test account is never used here.
 */
public class AccountPicker {

    private final List<SeedData.Account> hot = new ArrayList<>();
    private final List<SeedData.Account> busy = new ArrayList<>();
    private final List<SeedData.Account> quiet = new ArrayList<>();

    public AccountPicker() {
        Set<String> busyGroups = new java.util.HashSet<>();
        for (int g = 1; g <= (SeedData.GROUP_COUNT - 1) / 5; g++) {
            busyGroups.add(SeedData.groupId(g));
        }
        for (SeedData.Account a : SeedData.accounts()) {
            if (a.groupId().equals(SeedData.CONC_GROUP)) {
                continue;
            }
            if (a.groupId().equals(SeedData.HOT_GROUP)) {
                hot.add(a);
            } else if (busyGroups.contains(a.groupId())) {
                busy.add(a);
            } else {
                quiet.add(a);
            }
        }
    }

    public SeedData.Account next() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        int roll = r.nextInt(100);
        List<SeedData.Account> pool = roll < 10 ? hot : roll < 82 ? busy : quiet;
        return pool.get(r.nextInt(pool.size()));
    }
}
