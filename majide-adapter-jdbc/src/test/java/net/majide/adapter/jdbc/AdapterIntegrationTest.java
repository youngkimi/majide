package net.majide.adapter.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class AdapterIntegrationTest extends TestSupport {

    @Test
    @DisplayName("Flyway 마이그레이션 및 시드 확인")
    void migrationAndSeed() throws Exception {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT NAME, ENABLED FROM TB_JOB WHERE NAME='demo'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("demo", rs.getString(1));
                assertEquals("Y", rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    @DisplayName("FOR UPDATE SKIP LOCKED 동시 선점: 서로 다른 TASK_RUN을 가져온다")
    void concurrentClaimWithSkipLocked() throws Exception {
        // 사전 상태: V2에서 READY 2개 존재
        String claimSql = """
            SELECT ID FROM (
              SELECT ID FROM TB_TASK_RUN
              WHERE STATUS='READY'
              ORDER BY ID
            ) WHERE ROWNUM = 1
            FOR UPDATE SKIP LOCKED
            """;

        var pool = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);

        Future<Long> f1 = pool.submit(() -> claimOne(claimSql, ready));
        Future<Long> f2 = pool.submit(() -> claimOne(claimSql, ready));

        Long a = f1.get();
        Long b = f2.get();
        assertNotNull(a);
        assertNotNull(b);
        assertNotEquals(a, b, "두 스레드가 같은 행을 잠그면 안 됨");

        pool.shutdownNow();
    }

    private Long claimOne(String sql, CountDownLatch ready) throws Exception {
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            Long id;
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return null;
                    id = rs.getLong(1);
                }
            }
            // 선점 마킹 (RUNNING + 임차)
            try (PreparedStatement upd = c.prepareStatement(
                    "UPDATE TB_TASK_RUN SET STATUS='RUNNING', UPDATED_AT=SYSTIMESTAMP, LEASE_UNTIL=SYSTIMESTAMP + NUMTODSINTERVAL(60,'SECOND') WHERE ID=?")) {
                upd.setLong(1, id);
                upd.executeUpdate();
            }

            ready.countDown();
            ready.await();
            // 트랜잭션을 늦게 커밋하여 잠금 유지 → 동시성 시뮬레이션
            Thread.sleep(200);
            c.commit();
            return id;
        }
    }

    @Test
    @DisplayName("모든 TASK_RUN이 DONE이면 JOB_RUN을 DONE으로 승격")
    void promoteJobRunToDoneWhenAllTasksDone() throws Exception {
        long runId;
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            // RUNNING 상태인 run 하나 가져오기
            try (ResultSet rs = st.executeQuery("SELECT ID FROM TB_JOB_RUN WHERE STATUS='RUNNING' FETCH FIRST 1 ROWS ONLY")) {
                assertTrue(rs.next());
                runId = rs.getLong(1);
            }

            // 해당 run의 TASK_RUN 전부 DONE으로 수정
            try (PreparedStatement upd = c.prepareStatement("UPDATE TB_TASK_RUN SET STATUS='DONE', FINISHED_AT=SYSTIMESTAMP, UPDATED_AT=SYSTIMESTAMP WHERE JOB_RUN_ID=?")) {
                upd.setLong(1, runId);
                int cnt = upd.executeUpdate();
                assertTrue(cnt >= 1);
            }

            // JOB_RUN DONE 승격 SQL (RUNNING만 대상으로)
            String promoteSql = """
                UPDATE TB_JOB_RUN jr SET STATUS = 'DONE', FINISHED_AT = SYSTIMESTAMP
                WHERE jr.ID = ?
                  AND jr.STATUS = 'RUNNING'
                  AND (
                    (SELECT COUNT(*) FROM TB_TASK t WHERE t.JOB_ID = jr.JOB_ID)
                    =
                    (SELECT COUNT(*) FROM TB_TASK_RUN tr WHERE tr.JOB_RUN_ID = jr.ID AND tr.STATUS = 'DONE')
                  )
                """;

            try (PreparedStatement ps = c.prepareStatement(promoteSql)) {
                ps.setLong(1, runId);
                int changed = ps.executeUpdate();
                assertEquals(1, changed, "조건을 만족하면 정확히 1건 승격되어야 함");
            }

            // 검증
            try (PreparedStatement ps = c.prepareStatement("SELECT STATUS FROM TB_JOB_RUN WHERE ID=?")) {
                ps.setLong(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("DONE", rs.getString(1));
                }
            }
        }
    }
}