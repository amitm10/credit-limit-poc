package com.poc.credit.common.seed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Deterministic reference data shared by the limit loader (which writes it) and the gateway
 * simulator (which needs to know which account belongs to which group).
 *
 * <ul>
 *   <li>G0000 is the "mega fund" manager: 50 accounts of $10M each under a $100M group
 *       limit, so group-level rejections happen long before account-level ones.</li>
 *   <li>G0001..G1999 hold the other 19,950 accounts, about 10 each, with random limits.</li>
 *   <li>GCONC/ACC-CONC is a dedicated $10M account for the concurrency test; the simulator's
 *       normal flow never touches it.</li>
 * </ul>
 */
public final class SeedData {

    public static final int GROUP_COUNT = 2_000;
    public static final int ACCOUNT_COUNT = 20_000;
    public static final String HOT_GROUP = "G0000";
    public static final int HOT_ACCOUNTS = 50;
    public static final String CONC_GROUP = "GCONC";
    public static final String CONC_ACCOUNT = "ACC-CONC";

    private static final long USD = 100L; // cents

    public record Group(String groupId, long limitCents) {
    }

    public record Account(String accountId, String groupId, long limitCents) {
    }

    private static final List<Group> GROUPS;
    private static final List<Account> ACCOUNTS;

    static {
        Random random = new Random(42);
        List<Group> groups = new ArrayList<>(GROUP_COUNT + 1);
        List<Account> accounts = new ArrayList<>(ACCOUNT_COUNT + 1);

        groups.add(new Group(HOT_GROUP, 100_000_000L * USD));
        for (int i = 1; i <= HOT_ACCOUNTS; i++) {
            accounts.add(new Account(accountId(i), HOT_GROUP, 10_000_000L * USD));
        }
        for (int g = 1; g < GROUP_COUNT; g++) {
            groups.add(new Group(groupId(g), millions(random, 20, 500)));
        }
        for (int i = HOT_ACCOUNTS + 1; i <= ACCOUNT_COUNT; i++) {
            String group = groupId(1 + (i - HOT_ACCOUNTS - 1) % (GROUP_COUNT - 1));
            accounts.add(new Account(accountId(i), group, millions(random, 1, 50)));
        }
        groups.add(new Group(CONC_GROUP, 1_000_000_000L * USD));
        accounts.add(new Account(CONC_ACCOUNT, CONC_GROUP, 10_000_000L * USD));

        GROUPS = Collections.unmodifiableList(groups);
        ACCOUNTS = Collections.unmodifiableList(accounts);
    }

    private SeedData() {
    }

    public static List<Group> groups() {
        return GROUPS;
    }

    public static List<Account> accounts() {
        return ACCOUNTS;
    }

    public static String accountId(int i) {
        return String.format("ACC-%05d", i);
    }

    public static String groupId(int g) {
        return String.format("G%04d", g);
    }

    private static long millions(Random random, int min, int max) {
        return (min + random.nextInt(max - min + 1)) * 1_000_000L * USD;
    }
}
