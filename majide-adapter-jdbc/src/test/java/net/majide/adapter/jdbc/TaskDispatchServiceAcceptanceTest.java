package net.majide.adapter.jdbc;

import net.majide.adapter.jdbc.repo.*;
import net.majide.core.model.JobRun;
import net.majide.core.model.Task;
import net.majide.core.model.TaskRun;
import net.majide.core.service.RetryPolicy;
import net.majide.core.service.TaskDispatchService;
import net.majide.core.service.TaskGraphService;
import net.majide.core.spi.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TaskDispatchServiceAcceptanceTest extends TestSupport {

    TxRunner tx;
    TaskRepository tasks;
    TaskDependencyRepository deps;
    TaskRunRepository taskRuns;
    JobRepository jobs;
    JobRunRepository jobRuns;
    Clock clock;

    @BeforeAll
    void initAll() throws Exception {
        tx = new JdbcTxRunner(ds);
        tasks    = new JdbcTaskRepository(ds);
        deps     = new JdbcTaskDependencyRepository(ds);
        taskRuns = new JdbcTaskRunRepository(ds);
        jobs     = new JdbcJobRepository(ds);
        jobRuns  = new JdbcJobRunRepository(ds);
        clock    = Instant::now;

        tx.required(() -> { TxContext.get().createStatement().execute("ALTER SESSION SET TIME_ZONE='Asia/Seoul'"); return null; });
    }

    @BeforeEach
    void truncateAll() throws Exception {
        tx.required(() -> {
            try (var st = TxContext.get().createStatement()) {
                for (String t : new String[]{"TB_TASK_RUN","TB_JOB_RUN","TB_TASK_DEP","TB_TASK","TB_JOB"}) {
                    try { st.execute("TRUNCATE TABLE " + t); } catch (Exception ignore) { st.execute("DELETE FROM " + t); }
                }
            }
            return null;
        });
    }

    @Test
    void claimAndStart_heartbeat_failRetry_markDone_flow() throws Exception {
        // seed: job + tasks (t1 indegree 0, t2 indegree 1), dep t1->t2, jobRun + prepare
        long jobId = tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
                INSERT INTO TB_JOB(NAME,DESCRIPTION,CRON_EXPR,NEXT_DUE_AT,ENABLED,CREATED_AT,UPDATED_AT)
                VALUES('demo','demo','*/5 * * * *', CURRENT_TIMESTAMP, 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, new String[]{"ID"})) {
                ps.executeUpdate(); try (var k=ps.getGeneratedKeys()){k.next(); return k.getLong(1);}
            }
        });
        tx.required(() -> {
            tasks.upsert(new Task(null, jobId, "t1", "h1", null, null, "first", 0, clock.now(), clock.now()));
            tasks.upsert(new Task(null, jobId, "t2", "h2", null, null, "second", 1, clock.now(), clock.now()));
            var t1 = tasks.findByJobAndName(jobId, "t1").orElseThrow();
            var t2 = tasks.findByJobAndName(jobId, "t2").orElseThrow();
            deps.add(t1.id(), t2.id());
            return null;
        });
        long jobRunId = tx.required(() -> jobRuns.upsert(jobId, "rk-1", JobRun.Status.CREATED).id());
        new TaskGraphService(tasks, deps, taskRuns, tx, clock).prepareFor(jobId, jobRunId);

        var svc = new TaskDispatchService(taskRuns, tx, RetryPolicy.fixed(Duration.ofSeconds(10)));

        // 1) READY 하나 클레임 → RUNNING
        int claimed = svc.claimAndStartUpTo(1, Duration.ofSeconds(30));
        assertEquals(1, claimed);
        List<TaskRun> afterClaim = tx.required(() -> taskRuns.findAllByJobRun(jobRunId));
        TaskRun running = afterClaim.stream().filter(tr -> tr.status()== TaskRun.Status.RUNNING).findFirst().orElseThrow();

        // 2) heartbeat로 lease 연장
        var beforeLease = running.leaseUntil();
        svc.heartbeat(running.id(), Duration.ofSeconds(120));
        var afterLease = tx.required(() -> taskRuns.findById(running.id()).orElseThrow()).leaseUntil();
        assertTrue(afterLease.isAfter(beforeLease), "lease extended");

        // 3) 실패 → 백오프 READY
        svc.failAndRetry(running.id(), /*attempt*/ 1, "boom");
        TaskRun retried = tx.required(() -> taskRuns.findById(running.id()).orElseThrow());
        assertEquals(TaskRun.Status.READY, retried.status());
        assertNotNull(retried.availableAt());
        assertTrue(retried.availableAt().isAfter(clock.now().minusSeconds(1)));

        Thread.sleep(10_500);

        // 4) 다시 클레임하여 DONE 마킹
        assertEquals(1, svc.claimAndStartUpTo(1, Duration.ofSeconds(30)));
        TaskRun claimed2 = tx.required(() -> taskRuns.findById(running.id()).orElseThrow());
        assertEquals(TaskRun.Status.RUNNING, claimed2.status());
        svc.markDone(claimed2.id());
        TaskRun done = tx.required(() -> taskRuns.findById(claimed2.id()).orElseThrow());
        assertEquals(TaskRun.Status.DONE, done.status());
    }
}

