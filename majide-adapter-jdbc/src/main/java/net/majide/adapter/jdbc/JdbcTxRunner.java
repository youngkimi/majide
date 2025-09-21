package net.majide.adapter.jdbc;

import net.majide.core.spi.TxRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public final class JdbcTxRunner implements TxRunner {
    private final DataSource ds;

    public JdbcTxRunner(DataSource ds) { this.ds = ds; }

    @Override
    public <T> T required(Callable<T> body) throws Exception {
        Connection outer = TxContext.get();
        if (outer != null) {
            // 이미 진행 중인 트랜잭션에 참여
            return body.call();
        }
        try (Connection c = ds.getConnection()) {
            boolean prevAuto = c.getAutoCommit();
            c.setAutoCommit(false);
            TxContext.set(c);
            try {
                T r = body.call();
                c.commit();
                return r;
            } catch (Throwable t) {            // Throwable로 롤백 보장
                safeRollback(c);
                sneakyThrow(t);                 // 검사/비검사 구분없이 재던짐
                return null; // unreachable
            } finally {
                TxContext.clear();              // 반드시 해제
                try { c.setAutoCommit(prevAuto); } catch (SQLException ignore) {}
            }
        }
    }

    @Override
    public <T> T requiresNew(Callable<T> body) throws Exception {
        // 바깥 트랜잭션을 '정지' / 새 커넥션으로 대체 후, 종료 시 복원
        Connection suspended = TxContext.get(); // 있을 수도, 없을 수도
        try (Connection c = ds.getConnection()) {
            boolean prevAuto = c.getAutoCommit();
            c.setAutoCommit(false);

            TxContext.set(c);
            try {
                T r = body.call();
                c.commit();
                return r;
            } catch (Throwable t) {
                safeRollback(c);
                sneakyThrow(t);
                return null; // unreachable
            } finally {
                // 새 Tx 컨텍스트 해제
                TxContext.clear();
                try { c.setAutoCommit(prevAuto); } catch (SQLException ignore) {}
                // 바깥 컨텍스트 복원
                if (suspended != null) TxContext.set(suspended);
            }
        }
    }

    private static void safeRollback(Connection c) {
        try { c.rollback(); } catch (SQLException ignore) {}
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable t) throws E { throw (E) t; }
}
