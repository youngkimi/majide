package net.majide.adapter.jdbc;

import net.majide.adapter.jdbc.repo.JdbcWorkerSlotRepository;
import net.majide.core.identity.ops.WorkerSlotService;
import net.majide.core.model.WorkerSlot;
import net.majide.core.spi.TxRunner;
import net.majide.core.spi.WorkerSlotRepository;
import org.junit.jupiter.api.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WorkerSlotService 인수(E2E) 테스트 (TestSupport 상속)
 * - 매 테스트마다 TRUNCATE/DELETE로 격리 보장
 * - 임차된 workerId는 반환값으로 캡쳐해 단언(특정 id 가정 X)
 * - reclaimExpired는 명시 UPDATE로 만료 연출
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
class WorkerSlotServiceAcceptanceTest extends TestSupport {

    private TxRunner tx;
    private WorkerSlotRepository workerSlots;
    private WorkerSlotService service;

    @BeforeAll
    void initAll() throws Exception {
        tx = new JdbcTxRunner(ds);

        workerSlots = new JdbcWorkerSlotRepository(ds);
        service = new WorkerSlotService(workerSlots);

        // 세션 타임존 고정(선택)
        tx.required(() -> {
            try (var st = TxContext.get().createStatement()) {
                st.execute("ALTER SESSION SET TIME_ZONE = 'Asia/Seoul'");
            }
            return null;
        });
    }

    /** 매 테스트 시작 전에 테이블 비우기(격리 보장) */
    @BeforeEach
    void truncateWorkerSlots() throws Exception {
        tx.required(() -> {
            try (var st = TxContext.get().createStatement()) {
                // FK 제약이 없다면 TRUNCATE가 가장 빠름. 안 되면 DELETE 사용.
                try {
                    st.execute("TRUNCATE TABLE TB_WORKER_SLOT");
                } catch (Exception ignore) {
                    st.execute("DELETE FROM TB_WORKER_SLOT");
                }
            }
            return null;
        });
    }

    // ---------- helpers ----------

    /** 해당 workerId를 “가용 상태”로 보장(없으면 생성) */
    private void seedFreeSlot(int workerId) throws Exception {
        tx.required(() -> {
            try (PreparedStatement up = TxContext.get().prepareStatement("""
                MERGE INTO TB_WORKER_SLOT t
                USING (SELECT ? AS WORKER_ID FROM dual) s
                ON (t.WORKER_ID = s.WORKER_ID)
                WHEN MATCHED THEN UPDATE
                    SET t.INSTANCE_TOKEN = NULL,
                        t.LEASE_UNTIL   = NULL,
                        t.UPDATED_AT    = SYSTIMESTAMP
                WHEN NOT MATCHED THEN INSERT (WORKER_ID, INSTANCE_TOKEN, LEASE_UNTIL, UPDATED_AT)
                                     VALUES (?, NULL, NULL, SYSTIMESTAMP)
            """)) {
                up.setInt(1, workerId);
                up.setInt(2, workerId);
                up.executeUpdate();
            }
            return null;
        });
    }

    private Optional<WorkerSlot> findSlot(int workerId) throws Exception {
        return tx.required(() -> workerSlots.findById(workerId));
    }

    private int countLeased() throws Exception {
        return tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
                SELECT COUNT(*) FROM TB_WORKER_SLOT WHERE INSTANCE_TOKEN IS NOT NULL
            """); var rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        });
    }

    private int countLeased(int workerId) throws Exception {
        return tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
                SELECT COUNT(*) FROM TB_WORKER_SLOT
                 WHERE WORKER_ID = ? AND INSTANCE_TOKEN IS NOT NULL
            """)) {
                ps.setInt(1, workerId);
                try (var rs = ps.executeQuery()) { rs.next(); return rs.getInt(1); }
            }
        });
    }

    private Instant leaseUntilOf(int workerId) throws Exception {
        return tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
                SELECT LEASE_UNTIL FROM TB_WORKER_SLOT WHERE WORKER_ID=?
            """)) {
                ps.setInt(1, workerId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    var ts = rs.getTimestamp(1);
                    return ts == null ? null : ts.toInstant();
                }
            }
        });
    }

    // ---------- tests ----------

    @Test
    void a1_leaseOne_assignsTokenAndLease_whenFree() throws Exception {
        seedFreeSlot(101);            // 테이블엔 101 하나뿐

        String token = "hostA:1234:uuid-101";
        WorkerSlot leased = tx.required(() -> service.leaseOne(token, Duration.ofSeconds(30))).orElseThrow();

        // 동적으로 반환된 id를 기준으로 단언
        int wid = leased.workerId();
        assertEquals(token, leased.instanceToken());
        assertNotNull(leased.leaseUntil());

        assertEquals(1, countLeased(wid));
        assertEquals(1, countLeased());
    }

    @Test
    void a2_leaseOne_returnsEmpty_whenAlreadyLeased() throws Exception {
        seedFreeSlot(102);            // 테이블엔 102 하나뿐

        String t1 = "hostB:2222:uuid-102a";
        String t2 = "hostC:3333:uuid-102b";

        WorkerSlot first = tx.required(() -> service.leaseOne(t1, Duration.ofSeconds(60))).orElseThrow();
        assertEquals(1, countLeased(first.workerId()));
        assertEquals(1, countLeased());

        // 같은 테이블에 더 이상 가용 슬롯이 없으므로 empty 여야 함
        Optional<WorkerSlot> second = tx.required(() -> service.leaseOne(t2, Duration.ofSeconds(60)));
        assertTrue(second.isEmpty(), "already leased, should return empty");

        assertEquals(1, countLeased(first.workerId()));
        assertEquals(1, countLeased());
    }

    @Test
    void a3_heartbeat_extendsLease_onlyWhenTokenMatches() throws Exception {
        seedFreeSlot(103);

        String token = "hostD:4444:uuid-103";
        WorkerSlot slot = tx.required(() -> service.leaseOne(token, Duration.ofSeconds(5))).orElseThrow();
        int wid = slot.workerId();
        Instant before = leaseUntilOf(wid);

        // 올바른 토큰으로 연장
        tx.required(() -> { service.heartbeat(wid, token, Duration.ofSeconds(120)); return null; });
        Instant after = leaseUntilOf(wid);
        assertTrue(after.isAfter(before), "lease should be extended");

        // 잘못된 토큰 → 변화 없음
        tx.required(() -> { service.heartbeat(wid, "wrong-token", Duration.ofSeconds(120)); return null; });
        Instant afterWrong = leaseUntilOf(wid);
        assertEquals(after, afterWrong, "wrong token must not extend lease");

        assertEquals(1, countLeased(wid));
        assertEquals(1, countLeased());
    }

    @Test
    void a4_release_clearsTokenAndLease_onlyWhenTokenMatches() throws Exception {
        seedFreeSlot(104);

        String token = "hostE:5555:uuid-104";
        WorkerSlot leased = tx.required(() -> service.leaseOne(token, Duration.ofSeconds(30))).orElseThrow();
        int wid = leased.workerId();

        assertEquals(1, countLeased(wid));
        assertEquals(1, countLeased());

        // 잘못된 토큰 → 반납되지 않음
        tx.required(() -> { service.release(wid, "bad-token"); return null; });
        assertEquals(1, countLeased(wid));
        assertEquals(1, countLeased());

        // 올바른 토큰 → 성공
        tx.required(() -> { service.release(wid, token); return null; });
        assertEquals(0, countLeased(wid));
        assertEquals(0, countLeased());

        var slot = findSlot(wid).orElseThrow();
        assertNull(slot.instanceToken());
        assertNull(slot.leaseUntil());
    }

    @Test
    void a5_reclaimExpired_freesOnlyExpired_onMixedSlots() throws Exception {
        // 두 슬롯만 존재
        seedFreeSlot(105);
        seedFreeSlot(106);

        String t5 = "hostF:6666:uuid-105";
        String t6 = "hostG:7777:uuid-106";

        int wid5 = tx.required(() -> service.leaseOne(t5, Duration.ofSeconds(60))).orElseThrow().workerId();
        int wid6 = tx.required(() -> service.leaseOne(t6, Duration.ofSeconds(180))).orElseThrow().workerId();

        // BEFORE 덤프 (임차 직후)
        tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
            SELECT WORKER_ID, INSTANCE_TOKEN,
                   TO_CHAR(LEASE_UNTIL, 'YYYY-MM-DD HH24:MI:SS.FF3') AS LU
              FROM TB_WORKER_SLOT
             WHERE WORKER_ID IN (?, ?)
             ORDER BY WORKER_ID
        """)) {
                ps.setInt(1, wid5);
                ps.setInt(2, wid6);
                try (var rs = ps.executeQuery()) {
                    System.out.println("[BEFORE] ---");
                    while (rs.next()) {
                        System.out.printf("wid=%d token=%s lease=%s%n",
                                rs.getInt("WORKER_ID"),
                                rs.getString("INSTANCE_TOKEN"),
                                rs.getString("LU"));
                    }
                }
            }
            return null;
        });

        // wid5만 만료 상태로 조정(결정적)
        tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
            UPDATE TB_WORKER_SLOT
               SET LEASE_UNTIL = SYSTIMESTAMP - NUMTODSINTERVAL(10,'SECOND')
             WHERE WORKER_ID = ?
        """)) {
                ps.setInt(1, wid5);
                ps.executeUpdate();
            }
            return null;
        });

        // 만료 조정 확인 덤프
        tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
            SELECT WORKER_ID, INSTANCE_TOKEN,
                   TO_CHAR(LEASE_UNTIL, 'YYYY-MM-DD HH24:MI:SS.FF3') AS LU,
                   TO_CHAR(CAST(SYSTIMESTAMP AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS.FF3') AS NOWTS
              FROM TB_WORKER_SLOT
             WHERE WORKER_ID IN (?, ?)
             ORDER BY WORKER_ID
        """)) {
                ps.setInt(1, wid5);
                ps.setInt(2, wid6);
                try (var rs = ps.executeQuery()) {
                    System.out.println("[BEFORE-EXPIRE] ---");
                    while (rs.next()) {
                        System.out.printf("wid=%d token=%s lease=%s now=%s%n",
                                rs.getInt("WORKER_ID"),
                                rs.getString("INSTANCE_TOKEN"),
                                rs.getString("LU"),
                                rs.getString("NOWTS"));
                    }
                }
            }
            return null;
        });

        // 구현이 반드시 "만료된 것만 회수"하도록 되어 있어야 함:
        // WHERE INSTANCE_TOKEN IS NOT NULL AND LEASE_UNTIL IS NOT NULL AND LEASE_UNTIL <= SYSTIMESTAMP
        int reclaimed = tx.required(service::reclaimExpired);
        assertTrue(reclaimed >= 1, "at least one expired should be reclaimed");

        // AFTER 덤프
        tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
            SELECT WORKER_ID, INSTANCE_TOKEN,
                   TO_CHAR(LEASE_UNTIL, 'YYYY-MM-DD HH24:MI:SS.FF3') AS LU
              FROM TB_WORKER_SLOT
             WHERE WORKER_ID IN (?, ?)
             ORDER BY WORKER_ID
        """)) {
                ps.setInt(1, wid5);
                ps.setInt(2, wid6);
                try (var rs = ps.executeQuery()) {
                    System.out.println("[AFTER] ---");
                    while (rs.next()) {
                        System.out.printf("wid=%d token=%s lease=%s%n",
                                rs.getInt("WORKER_ID"),
                                rs.getString("INSTANCE_TOKEN"),
                                rs.getString("LU"));
                    }
                }
            }
            return null;
        });

        // 단언
        var s5 = findSlot(wid5).orElseThrow();
        assertNull(s5.instanceToken());
        assertNull(s5.leaseUntil());

        var s6 = findSlot(wid6).orElseThrow();
        assertEquals(t6, s6.instanceToken(), "non-expired must remain leased");
        assertNotNull(s6.leaseUntil());
    }
}
