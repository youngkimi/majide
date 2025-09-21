package net.majide.adapter.jdbc;

import net.majide.adapter.jdbc.repo.*;
import net.majide.core.model.JobRun;
import net.majide.core.model.Task;
import net.majide.core.model.TaskRun;
import net.majide.core.spi.*;
import org.junit.jupiter.api.*;

import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 병렬 경합 인수 테스트
 * - TestSupport를 상속 (컨테이너/DS/Flyway 준비)
 * - 모든 Repo는 TxContext 커넥션만 사용
 * - FOR UPDATE SKIP LOCKED, 원자적 UPDATE 승격(increment) 등 정석 패턴이 제대로 동작하는지 검증
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
class ConcurrentProcessingAcceptanceTest extends TestSupport {

    TxRunner tx;
    JobRepository jobs;
    JobRunRepository jobRuns;
    TaskRepository tasks;
    TaskDependencyRepository deps;
    TaskRunRepository taskRuns;

    Clock clock;

    @BeforeAll
    void initAll() throws Exception {
        tx = new JdbcTxRunner(ds);

        jobs     = new JdbcJobRepository(ds);
        jobRuns  = new JdbcJobRunRepository(ds);
        tasks    = new JdbcTaskRepository(ds);
        deps     = new JdbcTaskDependencyRepository(ds);
        taskRuns = new JdbcTaskRunRepository(ds);
        clock    = Instant::now;

        // 세션 TZ 고정(선택) — LTZ 컬럼과 CURRENT_TIMESTAMP를 일관 사용 중이라 큰 영향은 없음
        tx.required(() -> { TxContext.get().createStatement().execute("ALTER SESSION SET TIME_ZONE='Asia/Seoul'"); return null; });
    }

    @BeforeEach
    void truncateAll() throws Exception {
        tx.required(() -> {
            try (var st = TxContext.get().createStatement()) {
                for (String t : new String[]{"TB_TASK_RUN","TB_JOB_RUN","TB_TASK_DEP","TB_TASK","TB_JOB","TB_WORKER_SLOT"}) {
                    try { st.execute("TRUNCATE TABLE " + t); } catch (Exception ignore) { st.execute("DELETE FROM " + t); }
                }
            }
            return null;
        });
    }

    // ========== t1: due Job 경합 — 한 스레드만 선점 ==========
    @Test
    void t1_concurrent_claimDueJob_onlyOneWins() throws Exception {
        // seed: due Job 1건
        long jobId = tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
                INSERT INTO TB_JOB(NAME,DESCRIPTION,CRON_EXPR,NEXT_DUE_AT,ENABLED,CREATED_AT,UPDATED_AT)
                VALUES('demo','demo','*/5 * * * *', CURRENT_TIMESTAMP - NUMTODSINTERVAL(60,'SECOND'), 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, new String[]{"ID"})) {
                ps.executeUpdate(); try (var k = ps.getGeneratedKeys()) { k.next(); return k.getLong(1); }
            }
        });

        int threads = 6;
        ExecutorService es = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(es.submit(() -> {
                start.await();
                return tx.requiresNew(() -> jobs.claimDueJob(Duration.ofSeconds(30), "dispatcher-"+Thread.currentThread().getId()))
                        .isPresent();
            }));
        }
        start.countDown();

        int wins = 0;
        for (Future<Boolean> f : futures) wins += f.get() ? 1 : 0;
        es.shutdown();
        assertEquals(1, wins, "exactly one thread should win job claim");

        // 선점된 Job의 LEASE_UNTIL 확인
        var j = tx.required(() -> jobs.findById(jobId).orElseThrow());
        assertNotNull(j.leaseUntil());
    }

    // ========== t2: READY TaskRun 경합 — 한 스레드만 RUNNING ==========
    @Test
    void t2_concurrent_claimReady_onlyOneGetsRunning() throws Exception {
        // seed: job + task 1개 indegree=0 → prepare 로 READY 1건 생성
        long jobId = seedJob("demo2");
        tx.required(() -> {
            tasks.upsert(new Task(null, jobId, "t1", "h1", null, null, "one", 0, clock.now(), clock.now()));
            return null;
        });
        long runId = tx.required(() -> jobRuns.upsert(jobId, "rk-1", JobRun.Status.CREATED).id());
        new net.majide.core.service.TaskGraphService(tasks, deps, taskRuns, tx, clock).prepareFor(jobId, runId);

        int threads = 6;
        ExecutorService es = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Optional<TaskRun>>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(es.submit(() -> {
                start.await();
                return tx.requiresNew(() -> taskRuns.claimReady(Duration.ofSeconds(30), "w"));
            }));
        }
        start.countDown();

        int wins = 0;
        for (Future<Optional<TaskRun>> f : futures) if (f.get().isPresent()) wins++;
        es.shutdown();
        assertEquals(1, wins, "exactly one claimReady should succeed");

        // RUNNING 1건, READY 0건 확인
        var list = tx.required(() -> taskRuns.findAllByJobRun(runId));
        assertEquals(1, list.stream().filter(tr -> tr.status()== TaskRun.Status.RUNNING).count());
        assertEquals(0, list.stream().filter(tr -> tr.status()== TaskRun.Status.READY).count());
    }

    // ========== t3: 다중 선행 승격 — 동시 incrementDoneCount에도 READY 정확히 1회 승격 ==========
    @Test
    void t3_concurrent_promotion_multiPredecessors_exactlyOnce() throws Exception {
        // 그래프: t1,t2 -> t3 (precnt=2)
        long jobId = seedJob("demo3");
        tx.required(() -> {
            tasks.upsert(new Task(null, jobId, "t1", "h1", null, null, "first", 0, clock.now(), clock.now()));
            tasks.upsert(new Task(null, jobId, "t2", "h2", null, null, "second", 0, clock.now(), clock.now()));
            tasks.upsert(new Task(null, jobId, "t3", "h3", null, null, "third", 2, clock.now(), clock.now())); // indegree=2
            // deps: t1->t3, t2->t3
            var t1 = tasks.findByJobAndName(jobId, "t1").orElseThrow();
            var t2 = tasks.findByJobAndName(jobId, "t2").orElseThrow();
            var t3 = tasks.findByJobAndName(jobId, "t3").orElseThrow();
            deps.add(t1.id(), t3.id());
            deps.add(t2.id(), t3.id());
            return null;
        });
        long runId = tx.required(() -> jobRuns.upsert(jobId, "rk-1", JobRun.Status.CREATED).id());
        new net.majide.core.service.TaskGraphService(tasks, deps, taskRuns, tx, clock).prepareFor(jobId, runId);

        // t3 TaskRun id 조회 (BLOCKED 상태여야)
        TaskRun tr3 = tx.required(() -> {
            var all = taskRuns.findAllByJobRun(runId);
            return all.stream().filter(tr -> {
                try {
                    return "t3".equalsIgnoreCase(taskNameById(tr.taskId()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).findFirst().orElseThrow();
        });

        // 동시 두 번 incrementDoneCount → READY 승격이 정확히 1회
        ExecutorService es = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Callable<Void> inc = () -> {
            start.await();
            return tx.requiresNew(() -> { taskRuns.incrementDoneCount(tr3.id()); return null; });
        };
        Future<Void> f1 = es.submit(inc);
        Future<Void> f2 = es.submit(inc);
        start.countDown();
        f1.get(); f2.get(); es.shutdown();

        TaskRun after = tx.required(() -> taskRuns.findById(tr3.id()).orElseThrow());
        assertTrue(after.doneCnt() >= after.preCnt(), "done >= pre");
        assertEquals(TaskRun.Status.READY, after.status(), "promoted to READY exactly once");
        assertNotNull(after.availableAt(), "available_at set on promotion");
    }

    // ========== t4: READY 10건을 8스레드가 분산 Claim — 중복 없이 정확히 10건 ==========
    @Test
    void t4_parallel_claimReady_distributesWithoutDuplication() throws Exception {
        long jobId = seedJob("demo4");
        // TASK 10개 indegree=0
        tx.required(() -> {
            for (int i = 0; i < 10; i++) {
                tasks.upsert(new Task(null, jobId, "t"+i, "h", null, null, "t"+i, 0, clock.now(), clock.now()));
            }
            return null;
        });
        long runId = tx.required(() -> jobRuns.upsert(jobId, "rk-1", JobRun.Status.CREATED).id());
        new net.majide.core.service.TaskGraphService(tasks, deps, taskRuns, tx, clock).prepareFor(jobId, runId);

        ExecutorService es = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        Set<Long> claimedIds = ConcurrentHashMap.newKeySet();
        AtomicInteger totalClaims = new AtomicInteger(0);

        Runnable worker = () -> {
            try {
                start.await();
                while (true) {
                    Optional<TaskRun> opt = tx.requiresNew(() -> taskRuns.claimReady(Duration.ofSeconds(30), "w"));
                    if (opt.isEmpty()) break;
                    claimedIds.add(opt.get().id());
                    totalClaims.incrementAndGet();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        for (int i = 0; i < 8; i++) es.submit(worker);
        start.countDown();
        es.shutdown();
        assertTrue(es.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(10, claimedIds.size(), "no duplicates, all 10 claimed exactly once");
        assertEquals(10, totalClaims.get(), "exactly 10 successful claims");
        // READY 0, RUNNING 10
        var all = tx.required(() -> taskRuns.findAllByJobRun(runId));
        assertEquals(10, all.stream().filter(tr -> tr.status()== TaskRun.Status.RUNNING).count());
        assertEquals(0, all.stream().filter(tr -> tr.status()== TaskRun.Status.READY).count());
    }

    // ===== helpers =====

    private long seedJob(String name) throws Exception {
        return tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
                INSERT INTO TB_JOB(NAME,DESCRIPTION,CRON_EXPR,NEXT_DUE_AT,ENABLED,CREATED_AT,UPDATED_AT)
                VALUES(?, ?, '*/5 * * * *', CURRENT_TIMESTAMP, 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, new String[]{"ID"})) {
                ps.setString(1, name);
                ps.setString(2, name+" job");
                ps.executeUpdate();
                try (ResultSet k = ps.getGeneratedKeys()) { k.next(); return k.getLong(1); }
            }
        });
    }

    // Task 이름을 id로 찾는 간단 헬퍼 (테스트 용)
    private String taskNameById(long taskId) throws Exception {
        return tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("SELECT TASK_NAME FROM TB_TASK WHERE ID=?")) {
                ps.setLong(1, taskId);
                try (var rs = ps.executeQuery()) { rs.next(); return rs.getString(1); }
            }
        });
    }
}
