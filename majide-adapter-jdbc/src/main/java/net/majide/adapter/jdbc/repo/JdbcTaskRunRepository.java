package net.majide.adapter.jdbc.repo;

import net.majide.adapter.jdbc.JdbcUtil;
import net.majide.adapter.jdbc.TxContext;
import net.majide.adapter.jdbc.mapper.RowMappers;
import net.majide.core.model.TaskRun;
import net.majide.core.spi.TaskRunRepository;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public final class JdbcTaskRunRepository implements TaskRunRepository {
    private final DataSource ds;
    public JdbcTaskRunRepository(DataSource ds) { this.ds = ds; }

    private Connection mustConn() {
        Connection c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required");
        return c;
    }

    @Override
    public Optional<TaskRun> claimReady(Duration lease, String workerToken) throws Exception {
        Connection c = mustConn();

        // 1) 하나 픽업
        Long id = null;
        try (var ps = c.prepareStatement("""
            SELECT  tr.*
            FROM    TB_TASK_RUN tr
            WHERE   tr.ROWID IN (
                SELECT rid
                FROM (
                    SELECT  tr2.ROWID AS rid
                    FROM    TB_TASK_RUN tr2
                    WHERE   tr2.STATUS = 'READY'
                      AND   tr2.AVAILABLE_AT <= CURRENT_TIMESTAMP
                    ORDER BY tr2.AVAILABLE_AT ASC, tr2.ID ASC
                    FETCH FIRST 1 ROWS ONLY
                )
            )
            FOR UPDATE OF tr.STATUS SKIP LOCKED
        """)) {
            try (var rs = ps.executeQuery()) {
                if (rs.next()) id = rs.getLong(1);
            }
        }
        if (id == null) return Optional.empty();

        // 2) RUNNING 전환
        try (var up = c.prepareStatement("""
            UPDATE TB_TASK_RUN
               SET STATUS='RUNNING',
                   LEASE_UNTIL = CURRENT_TIMESTAMP + NUMTODSINTERVAL(?, 'SECOND'),
                   STARTED_AT  = COALESCE(STARTED_AT, CURRENT_TIMESTAMP),
                   UPDATED_AT  = CURRENT_TIMESTAMP
             WHERE ID = ?
        """)) {
            up.setInt(1, (int) lease.toSeconds());
            up.setLong(2, id);
            up.executeUpdate();
        }

        // 3) 로우 반환
        try (var sel = c.prepareStatement("SELECT * FROM TB_TASK_RUN WHERE ID=?")) {
            sel.setLong(1, id);
            try (var rs = sel.executeQuery()) {
                if (rs.next()) return Optional.of(RowMappers.toTaskRun(rs));
                return Optional.empty();
            }
        }
    }

    @Override
    public void heartbeat(long taskRunId, Duration lease) throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_TASK_RUN
               SET LEASE_UNTIL = CURRENT_TIMESTAMP + NUMTODSINTERVAL(?, 'SECOND'),
                   UPDATED_AT = CURRENT_TIMESTAMP
             WHERE ID = ? AND STATUS='RUNNING'
        """)) {
            ps.setInt(1, (int) lease.toSeconds());
            ps.setLong(2, taskRunId);
            ps.executeUpdate();
        }
    }

    @Override
    public void markDone(long taskRunId) throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_TASK_RUN
               SET STATUS='DONE',
                   FINISHED_AT = CURRENT_TIMESTAMP,
                   UPDATED_AT = CURRENT_TIMESTAMP
             WHERE ID=?
        """)) {
            ps.setLong(1, taskRunId);
            ps.executeUpdate();
        }
    }

    @Override
    public void retryWithBackoff(long taskRunId, Duration backoff, String lastError) throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_TASK_RUN
               SET STATUS='READY',
                   AVAILABLE_AT = CURRENT_TIMESTAMP + NUMTODSINTERVAL(?, 'SECOND'),
                   ATTEMPT = ATTEMPT + 1,
                   UPDATED_AT = CURRENT_TIMESTAMP,
                   LAST_ERROR = ?
             WHERE ID=?
        """)) {
            ps.setInt(1, (int) backoff.toSeconds());
            ps.setString(2, lastError);
            ps.setLong(3, taskRunId);
            ps.executeUpdate();
        }
    }

    @Override
    public void incrementDoneCount(long taskRunId) throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_TASK_RUN
               SET DONE_CNT   = DONE_CNT + 1,
                   STATUS     = CASE
                                  WHEN STATUS = 'BLOCKED' AND (DONE_CNT + 1) >= PRE_CNT THEN 'READY'
                                  ELSE STATUS
                                END,
                   AVAILABLE_AT = CASE
                                    WHEN STATUS = 'BLOCKED' AND (DONE_CNT + 1) >= PRE_CNT THEN CURRENT_TIMESTAMP
                                    ELSE AVAILABLE_AT
                                  END,
                   UPDATED_AT = CURRENT_TIMESTAMP
             WHERE ID = ?
        """)) {
            ps.setLong(1, taskRunId);
            ps.executeUpdate();
        }
        // 누락 보정은 Maintenance에서 일괄 승격
    }

    @Override
    public Optional<TaskRun> findById(long id) throws Exception {
        try (var ps = mustConn().prepareStatement("SELECT * FROM TB_TASK_RUN WHERE ID=?")) {
            ps.setLong(1, id);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(RowMappers.toTaskRun(rs)) : Optional.empty();
            }
        }
    }

    @Override
    public List<TaskRun> findAllByJobRun(long jobRunId) throws Exception {
        try (var ps = mustConn().prepareStatement("SELECT * FROM TB_TASK_RUN WHERE JOB_RUN_ID=? ORDER BY ID")) {
            ps.setLong(1, jobRunId);
            try (var rs = ps.executeQuery()) {
                var out = new ArrayList<TaskRun>();
                while (rs.next()) out.add(RowMappers.toTaskRun(rs));
                return out;
            }
        }
    }

    @Override
    public void createOrReset(long jobRunId, long taskId, int preCnt,
                              TaskRun.Status status, Instant available) throws Exception {
        try (var ps = mustConn().prepareStatement("""
            MERGE INTO TB_TASK_RUN tr
            USING (
                SELECT
                    ? AS job_run_id,
                    ? AS task_id,
                    ? AS pre_cnt,
                    ? AS status,
                    ? AS available_at
                FROM dual
            ) s
            ON (tr.JOB_RUN_ID = s.job_run_id AND tr.TASK_ID = s.task_id AND tr.ATTEMPT = 1)
            WHEN MATCHED THEN UPDATE SET
                tr.PRE_CNT      = s.pre_cnt,
                tr.DONE_CNT     = 0,
                tr.STATUS       = s.status,
                tr.AVAILABLE_AT = COALESCE(s.available_at, CASE WHEN s.pre_cnt = 0 THEN CURRENT_TIMESTAMP ELSE NULL END),
                tr.UPDATED_AT   = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT (
                -- ID 생략: IDENTITY가 자동 발번
                JOB_RUN_ID, TASK_ID, ATTEMPT, PRE_CNT, DONE_CNT, STATUS, AVAILABLE_AT, CREATED_AT, UPDATED_AT
            ) VALUES (
                s.job_run_id, s.task_id, 1, s.pre_cnt, 0, s.status,
                COALESCE(s.available_at, CASE WHEN s.pre_cnt = 0 THEN CURRENT_TIMESTAMP ELSE NULL END),
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
        """)) {
            // USING 파라미터
            ps.setLong(1, jobRunId);
            ps.setLong(2, taskId);
            ps.setInt(3, preCnt);
            ps.setString(4, status.code());             // 'READY' / 'BLOCKED' 등
            ps.setTimestamp(5, JdbcUtil.ts(available)); // null 허용
            ps.executeUpdate();
        }
    }

    // --- Maintenance 전용 메서드들 ---

    @Override
    public int recoverExpiredLeases(Duration backoff, String reason) throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_TASK_RUN
               SET STATUS='READY',
                   AVAILABLE_AT = CURRENT_TIMESTAMP + NUMTODSINTERVAL(?, 'SECOND'),
                   ATTEMPT = ATTEMPT + 1,
                   LEASE_UNTIL = NULL,
                   UPDATED_AT = CURRENT_TIMESTAMP,
                   LAST_ERROR = ?
             WHERE STATUS='RUNNING'
               AND LEASE_UNTIL IS NOT NULL
               AND LEASE_UNTIL <= CURRENT_TIMESTAMP
        """)) {
            ps.setInt(1, (int) backoff.toSeconds());
            ps.setString(2, reason);
            return ps.executeUpdate();
        }
    }

    @Override
    public int promoteUnblockedToReady() throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_TASK_RUN
               SET STATUS='READY',
                   AVAILABLE_AT = CURRENT_TIMESTAMP,
                   UPDATED_AT = CURRENT_TIMESTAMP
             WHERE STATUS='BLOCKED'
               AND DONE_CNT >= PRE_CNT
        """)) {
            return ps.executeUpdate();
        }
    }

    @Override
    public int archiveFinishedOlderThan(Instant threshold) throws Exception {
        // 간단히 플래그 없이 no-op로 시작하거나, 별도 아카이브 테이블 전략으로 확장
        try (var ps = mustConn().prepareStatement("""
            DELETE FROM TB_TASK_RUN
             WHERE (STATUS='DONE' OR STATUS='FAILED')
               AND FINISHED_AT IS NOT NULL
               AND FINISHED_AT < ?
        """)) {
            ps.setTimestamp(1, JdbcUtil.ts(threshold));
            return ps.executeUpdate();
        }
    }

    @Override
    public int normalizeReadyAvailability(Instant min, Instant max) throws Exception {
        try (var ps = mustConn().prepareStatement("""
            UPDATE TB_TASK_RUN
               SET AVAILABLE_AT = CASE
                   WHEN AVAILABLE_AT < ? THEN ?
                   WHEN AVAILABLE_AT > ? THEN ?
                   ELSE AVAILABLE_AT
               END,
               UPDATED_AT = CURRENT_TIMESTAMP
             WHERE STATUS='READY'
               AND AVAILABLE_AT IS NOT NULL
               AND (AVAILABLE_AT < ? OR AVAILABLE_AT > ?)
        """)) {
            Timestamp tmin = JdbcUtil.ts(min);
            Timestamp tmax = JdbcUtil.ts(max);
            ps.setTimestamp(1, tmin);
            ps.setTimestamp(2, tmin);
            ps.setTimestamp(3, tmax);
            ps.setTimestamp(4, tmax);
            ps.setTimestamp(5, tmin);
            ps.setTimestamp(6, tmax);
            return ps.executeUpdate();
        }
    }
}
