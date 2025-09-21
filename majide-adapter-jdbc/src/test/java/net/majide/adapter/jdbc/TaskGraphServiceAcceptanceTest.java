package net.majide.adapter.jdbc;

import net.majide.adapter.jdbc.repo.*;
import net.majide.core.model.Task;
import net.majide.core.model.TaskRun;
import net.majide.core.service.TaskGraphService;
import net.majide.core.spi.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TaskGraphServiceAcceptanceTest extends TestSupport {

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
    void prepare_setsBlockedAndReady_then_onPredecessorDone_promotesToReady() throws Exception {
        long jobId = tx.required(() -> {
            try (var ps = TxContext.get().prepareStatement("""
                INSERT INTO TB_JOB(NAME,DESCRIPTION,CRON_EXPR,NEXT_DUE_AT,ENABLED,CREATED_AT,UPDATED_AT)
                VALUES('demo','demo','*/5 * * * *', CURRENT_TIMESTAMP, 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, new String[]{"ID"})) {
                ps.executeUpdate(); try (var k=ps.getGeneratedKeys()){k.next(); return k.getLong(1);}
            }
        });
        // TASK 2 + DEP t1->t2
        tx.required(() -> {
            tasks.upsert(new Task(null, jobId, "t1", "h1", null, null, "first", 0, clock.now(), clock.now()));
            tasks.upsert(new Task(null, jobId, "t2", "h2", null, null, "second", 1, clock.now(), clock.now()));
            var t1 = tasks.findByJobAndName(jobId, "t1").orElseThrow();
            var t2 = tasks.findByJobAndName(jobId, "t2").orElseThrow();
            deps.add(t1.id(), t2.id());
            return null;
        });
        // JobRun
        long jobRunId = tx.required(() -> jobRuns.upsert(jobId, "rk-1", net.majide.core.model.JobRun.Status.CREATED).id());

        var graph = new TaskGraphService(tasks, deps, taskRuns, tx, clock);
        graph.prepareFor(jobId, jobRunId);

        // 초기 상태 확인
        List<TaskRun> list = tx.required(() -> taskRuns.findAllByJobRun(jobRunId));
        assertEquals(2, list.size());
        var ready = list.stream().filter(tr -> tr.status()== TaskRun.Status.READY).findFirst().orElseThrow();
        var blocked = list.stream().filter(tr -> tr.status()== TaskRun.Status.BLOCKED).findFirst().orElseThrow();

        // 선행 완료 반영: 후행(현재 BLOCKED)쪽 doneCnt++ → preCnt==doneCnt 이면 READY 승격되어야 함
        tx.required(() -> { taskRuns.incrementDoneCount(blocked.id()); return null; }); // TaskGraphService.onPredecessorDone 래핑과 동일
        // 리포 구현에서 preCnt==doneCnt 시 STATUS=READY, AVAILABLE_AT=CURRENT_TIMESTAMP 로 승격해야 함

        TaskRun after = tx.required(() -> taskRuns.findById(blocked.id()).orElseThrow());
        assertEquals(TaskRun.Status.READY, after.status(), "blocked -> READY promoted");
        assertNotNull(after.availableAt(), "available_at set on promotion");

        // READY였던 선행은 그대로 READY 유지
        TaskRun stillReady = tx.required(() -> taskRuns.findById(ready.id()).orElseThrow());
        assertEquals(TaskRun.Status.READY, stillReady.status());
    }
}

