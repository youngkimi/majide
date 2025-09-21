package net.majide.adapter.jdbc.repo;

import net.majide.adapter.jdbc.mapper.RowMappers;
import net.majide.adapter.jdbc.TxContext;
import net.majide.core.model.WorkerSlot;
import net.majide.core.spi.WorkerSlotRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.Optional;

public final class JdbcWorkerSlotRepository implements WorkerSlotRepository {
    private final DataSource ds;

    public JdbcWorkerSlotRepository(DataSource ds) {
        this.ds = ds;
    }

    // === utils ===
    private Connection mustConn() {
        Connection c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required (wrap with JdbcTxRunner)");
        return c;
    }

    // === interface impl ===

    /** 가용 슬롯 하나를 FOR UPDATE SKIP LOCKED로 집어와 임차 */
    @Override
    public Optional<WorkerSlot> leaseFreeSlot(String token, Duration lease) throws Exception {
        Connection c = mustConn();

        Integer workerId = null;
        // 1) 가용 슬롯 하나 락
        try (var ps = c.prepareStatement("""
            SELECT  ws.*
            FROM    TB_WORKER_SLOT ws
            WHERE   ws.ROWID IN (
                SELECT rid
                FROM (
                    SELECT  ws2.ROWID AS rid
                    FROM    TB_WORKER_SLOT ws2
                    WHERE  (ws2.LEASE_UNTIL IS NULL OR ws2.LEASE_UNTIL <= CURRENT_TIMESTAMP)
                      AND   ws2.INSTANCE_TOKEN IS NULL
                    ORDER BY ws2.WORKER_ID ASC
                    FETCH FIRST 1 ROWS ONLY
                )
            )
            FOR UPDATE OF ws.INSTANCE_TOKEN, ws.LEASE_UNTIL SKIP LOCKED
        """)) {
            try (var rs = ps.executeQuery()) {
                if (rs.next()) workerId = rs.getInt(1);
            }
        }
        if (workerId == null) return Optional.empty();

        // 2) 임차 확정
        try (var up = c.prepareStatement("""
            UPDATE TB_WORKER_SLOT
               SET INSTANCE_TOKEN = ?,
                   LEASE_UNTIL    = CURRENT_TIMESTAMP + NUMTODSINTERVAL(?, 'SECOND'),
                   HEARTBEAT_AT   = CURRENT_TIMESTAMP,
                   UPDATED_AT     = CURRENT_TIMESTAMP
             WHERE WORKER_ID = ?
        """)) {
            up.setString(1, token);
            up.setInt(2, (int) lease.toSeconds());
            up.setInt(3, workerId);
            up.executeUpdate();
        }

        // 3) 조회/반환
        try (var sel = c.prepareStatement("SELECT * FROM TB_WORKER_SLOT WHERE WORKER_ID=?")) {
            sel.setInt(1, workerId);
            try (var rs = sel.executeQuery()) {
                if (rs.next()) return Optional.of(RowMappers.toWorkerSlot(rs));
                return Optional.empty();
            }
        }
    }

    /** 하트비트로 LEASE 연장 (토큰 일치 검증) */
    @Override
    public void heartbeat(int workerId, String token, Duration lease) throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_WORKER_SLOT
               SET HEARTBEAT_AT = CURRENT_TIMESTAMP,
                   LEASE_UNTIL  = CURRENT_TIMESTAMP + NUMTODSINTERVAL(?, 'SECOND'),
                   UPDATED_AT   = CURRENT_TIMESTAMP
             WHERE WORKER_ID = ?
               AND INSTANCE_TOKEN = ?
        """)) {
            ps.setInt(1, (int) lease.toSeconds());
            ps.setInt(2, workerId);
            ps.setString(3, token);
            ps.executeUpdate();
        }
    }

    /** 반납 (토큰 일치 시에만) */
    @Override
    public void release(int workerId, String token) throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_WORKER_SLOT
               SET INSTANCE_TOKEN = NULL,
                   LEASE_UNTIL    = NULL,
                   UPDATED_AT     = CURRENT_TIMESTAMP
             WHERE WORKER_ID = ?
               AND INSTANCE_TOKEN = ?
        """)) {
            ps.setInt(1, workerId);
            ps.setString(2, token);
            ps.executeUpdate();
        }
    }

    /** 만료 슬롯 일괄 회수 */
    @Override
    public int reclaimExpired() throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_WORKER_SLOT
               SET INSTANCE_TOKEN = NULL,
                   LEASE_UNTIL    = NULL,
                   UPDATED_AT     = CURRENT_TIMESTAMP
             WHERE INSTANCE_TOKEN IS NOT NULL
                AND LEASE_UNTIL IS NOT NULL
                AND LEASE_UNTIL <= CURRENT_TIMESTAMP
        """)) {
            return ps.executeUpdate();
        }
    }

    @Override
    public Optional<WorkerSlot> findById(int workerId) throws Exception {
        try (var ps = mustConn().prepareStatement("SELECT * FROM TB_WORKER_SLOT WHERE WORKER_ID=?")) {
            ps.setInt(1, workerId);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(RowMappers.toWorkerSlot(rs)) : Optional.empty();
            }
        }
    }
}
