package net.majide.adapter.jdbc;

import net.majide.adapter.jdbc.repo.*;
import net.majide.core.model.Task;
import net.majide.core.model.TaskRun;
import net.majide.core.service.JobTickService;
import net.majide.core.service.TaskGraphService;
import net.majide.core.spi.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobTickServiceAcceptanceTest extends TestSupport {

    TxRunner tx;
    JobRepository jobs;
    JobRunRepository jobRuns;
    TaskRepository tasks;
    TaskDependencyRepository deps;
    TaskRunRepository taskRuns;

    Clock clock;
    CronCalculator cron;

    @BeforeAll
    void initAll() throws Exception {
        tx = new JdbcTxRunner(ds);

        jobs     = new JdbcJobRepository(ds);
        jobRuns  = new JdbcJobRunRepository(ds);
        tasks    = new JdbcTaskRepository(ds);
        deps     = new JdbcTaskDependencyRepository(ds);
        taskRuns = new JdbcTaskRunRepository(ds);

        clock = Instant::now;
        cron  = (from, expr, zone) -> from.plusSeconds(300); // 더미
        // 세션 TZ 고정(선택)
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
    void tickOnce_claimsDueJob_createsJobRun_preparesTasks_and_advancesCursor() throws Exception {
        // seed: JOB(due), TASK 2개(t1 indegree 0, t2 indegree 1), DEP t1->t2
        long jobId = tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
                INSERT INTO TB_JOB(NAME,DESCRIPTION,CRON_EXPR,NEXT_DUE_AT,ENABLED,CREATED_AT,UPDATED_AT)
                VALUES('demo','demo','*/5 * * * *', CURRENT_TIMESTAMP - NUMTODSINTERVAL(60,'SECOND'), 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, new String[]{"ID"})) {
                ps.executeUpdate();
                try (var k = ps.getGeneratedKeys()) { k.next(); return k.getLong(1); }
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

        // SUT
        var graph = new TaskGraphService(tasks, deps, taskRuns, tx, clock);
        var svc   = new JobTickService(jobs, jobRuns, graph, tx, clock, cron);

        Instant beforeNextDue = tx.required(() -> jobs.findById(jobId).orElseThrow().nextDueAt());

        svc.tickOnce(Duration.ofSeconds(5)); // claim + upsert run + prepare + advance

        // 검증: JobRun 1개 생성
        long jobRunId = tx.required(() -> {
            try (var rs = TxContext.get().createStatement().executeQuery("""
                SELECT ID FROM TB_JOB_RUN WHERE JOB_ID = (SELECT ID FROM TB_JOB WHERE NAME='demo')
                ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY
            """)) { rs.next(); return rs.getLong(1); }
        });

        // 검증: TaskRuns 2개 (READY 1, BLOCKED 1)
        List<TaskRun> trs = tx.required(() -> taskRuns.findAllByJobRun(jobRunId));
        assertEquals(2, trs.size());
        assertEquals(1, trs.stream().filter(tr -> tr.status()== TaskRun.Status.READY).count());
        assertEquals(1, trs.stream().filter(tr -> tr.status()== TaskRun.Status.BLOCKED).count());

        // 검증: 커서 전진
        Instant afterNextDue = tx.required(() -> jobs.findById(jobId).orElseThrow().nextDueAt());
        assertTrue(afterNextDue.isAfter(beforeNextDue), "next_due_at should advance");
    }
}
