package net.majide.adapter.jdbc;

import java.sql.Connection;

public final class TxContext {
    private static final ThreadLocal<Connection> LOCAL = new ThreadLocal<>();
    private TxContext() {}
    public static void set(Connection c) { LOCAL.set(c); }
    public static Connection get() { return LOCAL.get(); }
    public static void clear() { LOCAL.remove(); }
}