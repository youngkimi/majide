package net.majide.adapter.jdbc.mapper;

import net.majide.adapter.jdbc.JdbcUtil;
import net.majide.core.model.*;
import java.sql.*;

public final class RowMappers {
    private RowMappers() {}

    // --- Job ---
    public static Job toJob(ResultSet rs) throws SQLException {
        return new Job(
                rs.getLong("ID"),
                rs.getString("NAME"),
                rs.getString("DESCRIPTION"),
                rs.getString("CRON_EXPR"),
                rs.getTimestamp("NEXT_DUE_AT").toInstant(),
                JdbcUtil.toInstant(rs.getTimestamp("LEASE_UNTIL")),
                "Y".equals(rs.getString("ENABLED")),
                rs.getTimestamp("CREATED_AT").toInstant(),
                rs.getTimestamp("UPDATED_AT").toInstant()
        );
    }

    // --- JobRun ---
    public static JobRun toJobRun(ResultSet rs) throws SQLException {
        return new JobRun(
                rs.getLong("ID"),
                rs.getLong("JOB_ID"),
                rs.getString("RUN_KEY"),
                JobRun.Status.from(rs.getString("STATUS")),
                rs.getTimestamp("CREATED_AT").toInstant(),
                rs.getTimestamp("UPDATED_AT").toInstant(),
                rs.getTimestamp("DEADLINE_AT").toInstant(),
                JdbcUtil.toInstant(rs.getTimestamp("STARTED_AT")),
                JdbcUtil.toInstant(rs.getTimestamp("FINISHED_AT"))
        );
    }

    // --- Task ---
    public static Task toTask(ResultSet rs) throws SQLException {
        Integer indegree = rs.getInt("INDEGREE");
        if (rs.wasNull()) indegree = null;
        return new Task(
                rs.getLong("ID"),
                rs.getLong("JOB_ID"),
                rs.getString("TASK_NAME"),
                rs.getString("HANDLER_KEY"),
                rs.getString("CLASS_FQN"),
                rs.getString("METHOD_NAME"),
                rs.getString("DESCRIPTION"),
                indegree,
                rs.getTimestamp("CREATED_AT").toInstant(),
                rs.getTimestamp("UPDATED_AT").toInstant()
        );
    }

    // --- TaskRun
    public static TaskRun toTaskRun(ResultSet rs) throws SQLException {
        return new TaskRun(
                rs.getLong("ID"),
                rs.getLong("JOB_RUN_ID"),
                rs.getLong("TASK_ID"),
                TaskRun.Status.from(rs.getString("STATUS")),
                rs.getLong("ATTEMPT"),
                rs.getInt("PRE_CNT"),
                rs.getInt("DONE_CNT"),
                rs.getInt("WORKER_ID"),
                JdbcUtil.toInstant(rs.getTimestamp("AVAILABLE_AT")),
                JdbcUtil.toInstant(rs.getTimestamp("LEASE_UNTIL")),
                JdbcUtil.toInstant(rs.getTimestamp("STARTED_AT")),
                JdbcUtil.toInstant(rs.getTimestamp("FINISHED_AT")),
                rs.getTimestamp("CREATED_AT").toInstant(),
                rs.getTimestamp("UPDATED_AT").toInstant(),
                rs.getString("LAST_ERROR")
        );
    }

    // --- WorkerSlot ---
    public static WorkerSlot toWorkerSlot(ResultSet rs) throws SQLException {
        Integer id = rs.getInt("WORKER_ID");
        if (rs.wasNull()) id = null;
        return new WorkerSlot(
                id,
                rs.getString("INSTANCE_TOKEN"),
                JdbcUtil.toInstant(rs.getTimestamp("LEASE_UNTIL")),
                JdbcUtil.toInstant(rs.getTimestamp("HEARTBEAT_AT")),
                rs.getTimestamp("CREATED_AT").toInstant(),
                rs.getTimestamp("UPDATED_AT").toInstant()
        );
    }
}
