package net.majide.adapter.jdbc.repo;

import net.majide.adapter.jdbc.TxContext;
import net.majide.adapter.jdbc.mapper.RowMappers;
import net.majide.core.model.Task;
import net.majide.core.spi.TaskRepository;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class JdbcTaskRepository implements TaskRepository {
    private final DataSource ds;

    public JdbcTaskRepository(DataSource ds) {
        this.ds = ds;
    }

    private Connection mustConn() {
        var c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required (wrap with JdbcTxRunner)");
        return c;
    }

    private static Instant toInstant(Timestamp ts) { return ts == null ? null : ts.toInstant(); }

    @Override
    public Optional<Task> findByJobAndName(long jobId, String taskName) throws Exception {
        try (PreparedStatement ps = mustConn().prepareStatement(
                "SELECT * FROM TB_TASK WHERE JOB_ID=? AND TASK_NAME=?")) {
            ps.setLong(1, jobId);
            ps.setString(2, taskName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(RowMappers.toTask(rs)) : Optional.empty();
            }
        }
    }

    @Override
    public List<Task> findAllByJob(long jobId) throws Exception {
        try (PreparedStatement ps = mustConn().prepareStatement(
                "SELECT * FROM TB_TASK WHERE JOB_ID=? ORDER BY TASK_NAME")) {
            ps.setLong(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                List<Task> list = new ArrayList<>();
                while (rs.next()) list.add(RowMappers.toTask(rs));
                return list;
            }
        }
    }

    /**
     * (JOB_ID, TASK_NAME) 유니크 기반 멱등 upsert.
     * - 존재 시: handler/class/method/description/indegree 갱신 + UPDATED_AT bump
     * - 미존재 시: INSERT + CREATED_AT/UPDATED_AT 세팅
     */
    @Override
    public void upsert(Task task) throws Exception {
        if (task.jobId() == null || task.name() == null) {
            throw new IllegalArgumentException("jobId and name(taskName) are required for upsert");
        }
        try (PreparedStatement ps = mustConn().prepareStatement(
                """
                MERGE INTO TB_TASK d
                USING (SELECT ? AS JOB_ID, ? AS TASK_NAME FROM dual) s
                   ON (d.JOB_ID = s.JOB_ID AND d.TASK_NAME = s.TASK_NAME)
                 WHEN MATCHED THEN UPDATE SET
                       HANDLER_KEY = ?,
                       CLASS_FQN   = ?,
                       METHOD_NAME = ?,
                       DESCRIPTION = ?,
                       INDEGREE    = ?,
                       UPDATED_AT  = CURRENT_TIMESTAMP
                 WHEN NOT MATCHED THEN
                   INSERT (JOB_ID, TASK_NAME, HANDLER_KEY, CLASS_FQN, METHOD_NAME, DESCRIPTION, INDEGREE, CREATED_AT, UPDATED_AT)
                   VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
        )) {
            // MATCHED UPDATE params
            ps.setLong(1, task.jobId());
            ps.setString(2, task.name());
            ps.setString(3, task.handlerKey());
            ps.setString(4, task.classFqn());
            ps.setString(5, task.methodName());
            ps.setString(6, task.description());
            if (task.indegree() == null) ps.setNull(7, Types.INTEGER); else ps.setInt(7, task.indegree());

            // NOT MATCHED INSERT params
            ps.setLong(8, task.jobId());
            ps.setString(9, task.name());
            ps.setString(10, task.handlerKey());
            ps.setString(11, task.classFqn());
            ps.setString(12, task.methodName());
            ps.setString(13, task.description());
            if (task.indegree() == null) ps.setNull(14, Types.INTEGER); else ps.setInt(14, task.indegree());

            ps.executeUpdate();
        }
    }
}
