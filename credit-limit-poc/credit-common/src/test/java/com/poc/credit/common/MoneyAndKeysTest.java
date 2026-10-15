package com.poc.credit.common;

import com.poc.credit.common.api.CreditCheckRequest;
import com.poc.credit.common.api.RequestHash;
import com.poc.credit.common.model.Side;
import com.poc.credit.common.redis.Keys;
import com.poc.credit.common.seed.SeedData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MoneyAndKeysTest {

    @Test
    void notionalIsRoundedHalfUpToTheCent() {
        assertThat(Money.notionalCents(100_000, new BigDecimal("54.20"))).isEqualTo(542_000_000L);
        assertThat(Money.notionalCents(3, new BigDecimal("0.005"))).isEqualTo(2L); // 0.015 -> 0.02
        assertThat(Money.toUsd(542_000_000L)).isEqualByComparingTo("5420000.00");
    }

    @Test
    void requestHashIgnoresPriceFormattingButNotContent() {
        var a = new CreditCheckRequest("O1", "ACC-1", Side.BUY, 100, new BigDecimal("54.2"));
        var b = new CreditCheckRequest("O1", "ACC-1", Side.BUY, 100, new BigDecimal("54.200"));
        var c = new CreditCheckRequest("O1", "ACC-1", Side.BUY, 101, new BigDecimal("54.2"));
        assertThat(RequestHash.of(a)).isEqualTo(RequestHash.of(b)).isNotEqualTo(RequestHash.of(c));
    }

    @Test
    void allKeysOfAGroupShareOneHashTag() {
        assertThat(Keys.scriptKeys("G0001", "ACC-00051", "ORD-1"))
                .allMatch(k -> k.contains("{G0001}"));
    }

    @Test
    void unsafeIdentifiersAreRejected() {
        assertThatThrownBy(() -> Keys.accountLimit("G1", "ACC}{x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Keys.reservation("G1", "pending")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void seedDataIsDeterministicAndShaped() {
        assertThat(SeedData.accounts()).hasSize(SeedData.ACCOUNT_COUNT + 1);
        assertThat(SeedData.groups()).hasSize(SeedData.GROUP_COUNT + 1);
        assertThat(SeedData.accounts().stream().filter(a -> a.groupId().equals(SeedData.HOT_GROUP)))
                .hasSize(SeedData.HOT_ACCOUNTS);
        assertThat(SeedData.accounts().get(100)).isEqualTo(SeedData.accounts().get(100));
    }
}
