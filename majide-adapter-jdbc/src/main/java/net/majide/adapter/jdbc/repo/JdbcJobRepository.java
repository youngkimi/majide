package net.majide.adapter.jdbc.repo;

import net.majide.adapter.jdbc.TxContext;
import net.majide.adapter.jdbc.mapper.RowMappers;
import net.majide.core.model.Job;
import net.majide.core.spi.JobRepository;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public final class JdbcJobRepository implements JobRepository {
    private final DataSource ds;

    public JdbcJobRepository(DataSource ds) { this.ds = ds; }

    @Override
    public Optional<Job> claimDueJob(Duration lease, String owner) throws Exception {
        Connection c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required");

        // due 하나 집어서 선점
        try (var ps = c.prepareStatement("""
            SELECT  j.*
            FROM    TB_JOB j
            WHERE   j.ROWID IN (
                SELECT rid
                FROM (
                    SELECT  j2.ROWID AS rid
                    FROM    TB_JOB j2
                    WHERE   j2.ENABLED = 'Y'
                      AND   j2.NEXT_DUE_AT <= CURRENT_TIMESTAMP
                      AND  (j2.LEASE_UNTIL IS NULL OR j2.LEASE_UNTIL <= CURRENT_TIMESTAMP)
                    ORDER BY j2.NEXT_DUE_AT ASC, j2.ID ASC
                    FETCH FIRST 1 ROWS ONLY
                )
            )
            FOR UPDATE OF j.LEASE_UNTIL SKIP LOCKED
        """)) {
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                long id = rs.getLong("ID");
                try (var upd = c.prepareStatement("""
                    UPDATE TB_JOB
                       SET LEASE_UNTIL = CURRENT_TIMESTAMP + NUMTODSINTERVAL(?, 'SECOND'),
                           UPDATED_AT  = CURRENT_TIMESTAMP
                     WHERE ID = ?
                """)) {
                    upd.setInt(1, (int) lease.toSeconds());
                    upd.setLong(2, id);
                    upd.executeUpdate();
                }
                return Optional.of(RowMappers.toJob(rs));
            }
        }
    }

    @Override
    public void advanceCursor(long jobId, Instant nextDueAt) throws Exception {
        Connection c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required");
        try (var ps = c.prepareStatement("""
            UPDATE TB_JOB
               SET NEXT_DUE_AT=?, LEASE_UNTIL=NULL, UPDATED_AT=CURRENT_TIMESTAMP
             WHERE ID=?
        """)) {
            ps.setTimestamp(1, java.sql.Timestamp.from(nextDueAt));
            ps.setLong(2, jobId);
            ps.executeUpdate();
        }
    }

    @Override
    public Optional<Job> findById(long id) throws Exception {
        final Connection c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required");
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT *
                FROM TB_JOB
                WHERE ID = ?
            """)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(RowMappers.toJob(rs));
                // RowMappers 없으면 직접 매핑:
                // return Optional.of(mapRow(rs));
            }
        }
    }

    @Override
    public Optional<Job> findByName(String name) throws Exception {
        final Connection c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required");
        try (PreparedStatement ps = c.prepareStatement("""
                SELECT *
                FROM TB_JOB
                WHERE UPPER(NAME) = UPPER(?)
            """)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(RowMappers.toJob(rs));
                // RowMappers 없으면 직접 매핑:
                // return Optional.of(mapRow(rs));
            }
        }
    }

    @Override
    public void save(Job job) throws Exception {
        // 단순 UPDATE 버전 (ID 필수).
        // Insert/upsert가 필요하면 별도 메서드로 분리해서 getGeneratedKeys로 ID를 반환하는 걸 추천.
        final Connection c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required");

        try (PreparedStatement ps = c.prepareStatement("""
                UPDATE TB_JOB
                   SET NAME        = ?,
                       DESCRIPTION = ?,
                       CRON_EXPR   = ?,
                       NEXT_DUE_AT = ?,
                       ENABLED     = ?,
                       LEASE_UNTIL = ?,
                       UPDATED_AT  = CURRENT_TIMESTAMP
                 WHERE ID = ?
            """)) {
            // 아래는 Job 구조에 맞춰 set 해줘. (예시는 record/게터 가정)
            ps.setString(1, job.name());
            ps.setString(2, job.description());
            ps.setString(3, job.cronExpr());
            // java.time.Instant → java.sql.Timestamp
            ps.setTimestamp(4, job.nextDueAt() == null ? null : java.sql.Timestamp.from(job.nextDueAt()));
            ps.setString(5, job.enabled() ? "Y" : "N");
            ps.setTimestamp(6, job.leaseUntil() == null ? null : java.sql.Timestamp.from(job.leaseUntil()));
            ps.setLong(7, job.id());
            int updated = ps.executeUpdate();
            if (updated == 0) {
                // 존재하지 않으면 예외로 알려주거나, 여기서 INSERT 로직 수행(선호하는 전략으로)
                throw new IllegalStateException("TB_JOB not found for ID=" + job.id());
            }
        }
    }

    @Override
    public Job upsert(String name, String description, String cronExpr, Instant nextDueAt, boolean enabled) throws Exception {
        // Oracle MERGE (name 유니크 기준)
        var sql = """
            MERGE INTO TB_JOB d
            USING (SELECT ? NAME FROM dual) s
               ON (d.NAME = s.NAME)
            WHEN MATCHED THEN UPDATE SET
                 DESCRIPTION = ?,
                 CRON_EXPR   = ?,
                 NEXT_DUE_AT = ?,
                 ENABLED     = ?,
                 UPDATED_AT  = SYSTIMESTAMP
            WHEN NOT MATCHED THEN INSERT
                 (NAME, DESCRIPTION, CRON_EXPR, NEXT_DUE_AT, ENABLED, CREATED_AT, UPDATED_AT)
            VALUES (?,    ?,           ?,         ?,          ?,       SYSTIMESTAMP, SYSTIMESTAMP)
            """;

        try (var ps = mustConn().prepareStatement(sql)) {
            int i = 1;
            ps.setString(i++, name);
            ps.setString(i++, description);
            ps.setString(i++, cronExpr);
            ps.setTimestamp(i++, Timestamp.from(nextDueAt));
            ps.setString(i++, enabled ? "Y" : "N");
            ps.setString(i++, name);
            ps.setString(i++, description);
            ps.setString(i++, cronExpr);
            ps.setTimestamp(i++, Timestamp.from(nextDueAt));
            ps.setString(i++, enabled ? "Y" : "N");
            ps.executeUpdate();
        }
        // 갱신된 행을 다시 로드해서 반환
        return findByName(name).orElseThrow(() -> new IllegalStateException("upsert failed to load job: " + name));
    }

    private Connection mustConn() {
        var c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required (wrap with JdbcTxRunner)");
        return c;
    }

}
