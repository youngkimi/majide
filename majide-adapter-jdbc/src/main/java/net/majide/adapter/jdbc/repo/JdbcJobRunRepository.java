package net.majide.adapter.jdbc.repo;

import net.majide.adapter.jdbc.TxContext;
import net.majide.adapter.jdbc.mapper.RowMappers;
import net.majide.core.model.JobRun;
import net.majide.core.spi.JobRunRepository;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Optional;

public final class JdbcJobRunRepository implements JobRunRepository {
    private final DataSource ds;

    public JdbcJobRunRepository(DataSource ds) {
        this.ds = ds;
    }

    private Connection mustConn() {
        var c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required (wrap with JdbcTxRunner)");
        return c;
    }

    private static java.time.Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    /**
     * (JOB_ID, RUN_KEY) 유니크 기반 멱등 업서트.
     * - 존재하면 상태는 기존 유지(업데이트 없음) — 필요 시 정책에 맞게 UPDATE로 바꿀 수 있음.
     * - 없으면 INSERT(initialStatus).
     * - 반환은 최신 행을 SELECT해서 매핑.
     */
    @Override
    public JobRun upsert(long jobId, String runKey, JobRun.Status initialStatus) throws Exception {
        try (PreparedStatement ps = mustConn().prepareStatement(
                """
                MERGE INTO TB_JOB_RUN d
                USING (SELECT ? JOB_ID, ? RUN_KEY FROM dual) s
                   ON (d.JOB_ID = s.JOB_ID AND d.RUN_KEY = s.RUN_KEY)
                 WHEN NOT MATCHED THEN
                   INSERT (JOB_ID, RUN_KEY, STATUS, CREATED_AT, UPDATED_AT, DEADLINE_AT)
                   VALUES (s.JOB_ID, s.RUN_KEY, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + NUMTODSINTERVAL(3600,'SECOND'))
                """
        )) {
            ps.setLong(1, jobId);
            ps.setString(2, runKey);
            ps.setString(3, initialStatus.code());
            ps.executeUpdate();
        }
        // 멱등 결과 읽어오기
        return findByJobAndRunKey(jobId, runKey)
                .orElseThrow(() -> new IllegalStateException("JobRun upsert failed unexpectedly"));
    }

    @Override
    public Optional<JobRun> findByJobAndRunKey(long jobId, String runKey) throws Exception {
        try (PreparedStatement ps = mustConn().prepareStatement(
                "SELECT * FROM TB_JOB_RUN WHERE JOB_ID=? AND RUN_KEY=?"
        )) {
            ps.setLong(1, jobId);
            ps.setString(2, runKey);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return Optional.of(RowMappers.toJobRun(rs));
                return Optional.empty();
            }
        }
    }

    /** 시작 마킹: RUNNING 전환 + STARTED_AT 최초 세팅 */
    @Override
    public void markStarted(long jobRunId) throws Exception {
        try (PreparedStatement ps = mustConn().prepareStatement(
                """
                UPDATE TB_JOB_RUN
                   SET STATUS     = 'RUNNING',
                       STARTED_AT = COALESCE(STARTED_AT, CURRENT_TIMESTAMP),
                       UPDATED_AT = CURRENT_TIMESTAMP
                 WHERE ID = ?
                """
        )) {
            ps.setLong(1, jobRunId);
            ps.executeUpdate();
        }
    }

    /** 종료 마킹: DONE/FAILED + FINISHED_AT */
    @Override
    public void markFinished(long jobRunId, boolean success) throws Exception {
        String status = success ? "DONE" : "FAILED";
        try (PreparedStatement ps = mustConn().prepareStatement(
                """
                UPDATE TB_JOB_RUN
                   SET STATUS      = ?,
                       FINISHED_AT = CURRENT_TIMESTAMP,
                       UPDATED_AT  = CURRENT_TIMESTAMP
                 WHERE ID = ?
                """
        )) {
            ps.setString(1, status);
            ps.setLong(2, jobRunId);
            ps.executeUpdate();
        }
    }
}
