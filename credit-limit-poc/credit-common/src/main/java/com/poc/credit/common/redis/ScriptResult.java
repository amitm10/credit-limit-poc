package com.poc.credit.common.redis;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Parsed reply of a credit Lua script:
 * {status, detail, accountHeadroomCents, groupHeadroomCents, field1, value1, field2, value2, ...}.
 * The trailing pairs are the reservation hash after the script ran (empty if it has none).
 */
public record ScriptResult(String status,
                           String detail,
                           Long accountHeadroomCents,
                           Long groupHeadroomCents,
                           Map<String, String> reservation) {

    public static ScriptResult parse(List<?> reply) {
        if (reply == null || reply.size() < 4) {
            throw new IllegalStateException("Unexpected script reply: " + reply);
        }
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 4; i + 1 < reply.size(); i += 2) {
            fields.put(str(reply.get(i)), str(reply.get(i + 1)));
        }
        return new ScriptResult(str(reply.get(0)), str(reply.get(1)),
                num(reply.get(2)), num(reply.get(3)), Collections.unmodifiableMap(fields));
    }

    public boolean is(String expected) {
        return expected.equals(status);
    }

    public boolean hasReservation() {
        return !reservation.isEmpty();
    }

    public String field(String name) {
        return reservation.get(name);
    }

    public long longField(String name) {
        String v = reservation.get(name);
        return v == null || v.isEmpty() ? 0L : Long.parseLong(v);
    }

    private static String str(Object o) {
        return o == null ? "" : Objects.toString(o);
    }

    private static Long num(Object o) {
        String s = str(o);
        return s.isEmpty() ? null : Long.valueOf(s);
    }
}
