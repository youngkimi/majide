package net.majide.adapter.jdbc.repo;

import net.majide.adapter.jdbc.TxContext;
import net.majide.core.spi.TaskDependencyRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public final class JdbcTaskDependencyRepository implements TaskDependencyRepository {
    private final DataSource ds;

    public JdbcTaskDependencyRepository(DataSource ds) {
        this.ds = ds;
    }

    private Connection mustConn() {
        var c = TxContext.get();
        if (c == null) throw new IllegalStateException("TxContext required (wrap with JdbcTxRunner)");
        return c;
    }

    /** 멱등 추가: UX_TASK_DEP_UNIQ(pre,post) 기반 MERGE */
    @Override
    public void add(long preTaskId, long postTaskId) throws Exception {
        try (PreparedStatement ps = mustConn().prepareStatement(
                """
                MERGE INTO TB_TASK_DEP d
                USING (SELECT ? PRE_TASK_ID, ? POST_TASK_ID FROM dual) s
                   ON (d.PRE_TASK_ID = s.PRE_TASK_ID AND d.POST_TASK_ID = s.POST_TASK_ID)
                 WHEN NOT MATCHED THEN
                   INSERT (PRE_TASK_ID, POST_TASK_ID, CREATED_AT)
                   VALUES (s.PRE_TASK_ID, s.POST_TASK_ID, CURRENT_TIMESTAMP)
                """
        )) {
            ps.setLong(1, preTaskId);
            ps.setLong(2, postTaskId);
            ps.executeUpdate();
        }
    }

    @Override
    public void remove(long preTaskId, long postTaskId) throws Exception {
        try (PreparedStatement ps = mustConn().prepareStatement(
                "DELETE FROM TB_TASK_DEP WHERE PRE_TASK_ID=? AND POST_TASK_ID=?"
        )) {
            ps.setLong(1, preTaskId);
            ps.setLong(2, postTaskId);
            ps.executeUpdate();
        }
    }

    /** 주어진 taskId의 선행(predecessors) 목록 = PRE_TASK_ID */
    @Override
    public List<Long> findPredecessors(long taskId) throws Exception {
        try (PreparedStatement ps = mustConn().prepareStatement(
                "SELECT PRE_TASK_ID FROM TB_TASK_DEP WHERE POST_TASK_ID=? ORDER BY PRE_TASK_ID"
        )) {
            ps.setLong(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                List<Long> list = new ArrayList<>();
                while (rs.next()) list.add(rs.getLong(1));
                return list;
            }
        }
    }

    /** 주어진 taskId의 후행(successors) 목록 = POST_TASK_ID */
    @Override
    public List<Long> findSuccessors(long taskId) throws Exception {
        try (PreparedStatement ps = mustConn().prepareStatement(
                "SELECT POST_TASK_ID FROM TB_TASK_DEP WHERE PRE_TASK_ID=? ORDER BY POST_TASK_ID"
        )) {
            ps.setLong(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                List<Long> list = new ArrayList<>();
                while (rs.next()) list.add(rs.getLong(1));
                return list;
            }
        }
    }
}
