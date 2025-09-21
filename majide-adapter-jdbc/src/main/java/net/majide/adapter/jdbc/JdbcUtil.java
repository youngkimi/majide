package net.majide.adapter.jdbc;

import java.sql.Timestamp;
import java.time.Instant;

public final class JdbcUtil {
    private JdbcUtil() {}

    public static Timestamp ts(Instant i) { return i == null ? null : Timestamp.from(i); }

    public static Instant toInstant(Timestamp ts) { return ts == null ? null : ts.toInstant(); }
}