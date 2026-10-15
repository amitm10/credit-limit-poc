package com.poc.credit.common.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Fingerprint of a check request. A retry with the same order ID must carry the same
 * fingerprint; a different one means the caller reused an order ID for a different order.
 */
public final class RequestHash {

    private RequestHash() {
    }

    public static String of(CreditCheckRequest r) {
        String canonical = String.join("|", r.orderId(), r.accountId(), r.side().name(),
                Long.toString(r.quantity()), r.price().stripTrailingZeros().toPlainString());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
