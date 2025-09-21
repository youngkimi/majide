package net.majide.app;

import net.majide.core.model.Task;
import net.majide.core.model.TaskRun;
import net.majide.core.service.Orchestrator;
import net.majide.core.service.TaskDispatchService;
import net.majide.core.spi.TaskDependencyRepository;
import net.majide.core.spi.TaskRepository;
import net.majide.core.spi.TaskRunRepository;
import net.majide.core.spi.TxRunner;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class SchedulingFlowIT {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-xe:21.3.0")
            .withReuse(true);

    @DynamicPropertySource
    static void dbProps(DynamicPropertyRegistry r) {
        // Boot DataSource & Flyway가 Testcontainers DB로 붙도록
        r.add("spring.datasource.url", oracle::getJdbcUrl);
        r.add("spring.datasource.username", oracle::getUsername);
        r.add("spring.datasource.password", oracle::getPassword);
        r.add("spring.datasource.driver-class-name", () -> "oracle.jdbc.OracleDriver");
        r.add("spring.flyway.enabled", () -> true);
        r.add("spring.flyway.locations", () -> "classpath:db/migration/oracle");
        // 스케줄 주기는 기본값으로 두되, 테스트 시간을 줄이고 싶으면 아래 주석 해제
        // r.add("majide.tick.delay-ms", () -> "500");
        // r.add("majide.maintenance.delay-ms", () -> "1000");
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired TxRunner tx;
    @Autowired TaskRepository tasks;
    @Autowired TaskDependencyRepository deps;
    @Autowired TaskDispatchService taskDispatchService;
    @Autowired TaskRunRepository taskRuns;
    @Autowired Orchestrator orchestrator;

    ExecutorService workerPool;

    @BeforeEach
    void setUp() {
        workerPool = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        workerPool.shutdownNow();
    }

    @Test
    void job_and_tasks_are_processed_in_dependency_order() throws Exception {
        // 1) 시드: 잡 1개(demo), 태스크 2개(t1 -> t2)
        seedDemoJobAndTasks();

        // 2) “가짜 워커” 실행: READY를 RUNNING으로 클레임한 후 즉시 DONE 처리
        //    - 실제 핸들러가 없으니, RUNNING 상태를 주기적으로 스캔해서 markDone 처리
        startFakeWorker();

        // 3) 대기: 두 태스크 모두 DONE 될 때까지 (최대 30초)
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var rows = jdbc.queryForList("""
                SELECT tr.STATUS, t.TASK_NAME, tr.STARTED_AT, tr.FINISHED_AT, tr.PRE_CNT, tr.DONE_CNT
                FROM TB_TASK_RUN tr
                JOIN TB_TASK t ON tr.TASK_ID = t.ID
                ORDER BY tr.ID
            """);
            // 두 개의 TaskRun이 있고 둘 다 DONE 이어야 한다
            assertThat(rows).hasSize(2);
            assertThat(rows.stream().map(r -> (String) r.get("STATUS")))
                    .containsExactlyInAnyOrder("DONE", "DONE");

            // 의존 순서 검증: t1이 먼저 시작/끝나고, t2가 그 후에 READY→RUNNING→DONE
            var byName = rows.stream().collect(Collectors.toMap(
                    r -> (String) r.get("TASK_NAME"),
                    r -> r
            ));
            var t1 = byName.get("t1");
            var t2 = byName.get("t2");
            assertThat((java.sql.Timestamp) t1.get("FINISHED_AT"))
                    .isNotNull();
            assertThat((java.sql.Timestamp) t2.get("FINISHED_AT"))
                    .isNotNull();
            // t2의 시작은 t1의 완료 이후여야 함 (느슨한 검증)
            assertThat(((java.sql.Timestamp) t2.get("STARTED_AT")).toInstant())
                    .isAfterOrEqualTo(((java.sql.Timestamp) t1.get("FINISHED_AT")).toInstant());
        });
    }

    // --- helpers ---

    private void seedDemoJobAndTasks() throws Exception {
        // JOB upsert: 바로 due 처리되도록 NEXT_DUE_AT 과거로
        jdbc.update("""
            MERGE INTO TB_JOB d
            USING (SELECT 'demo' NAME FROM dual) s
               ON (d.NAME = s.NAME)
            WHEN MATCHED THEN UPDATE SET
                 CRON_EXPR   = '*/5 * * * * ?',  -- 5필드 기준(분 단위); 여러분 환경이 Quartz 6필드면 '*/5 * * * * ?'
                 DESCRIPTION = 'demo job',
                 NEXT_DUE_AT = SYSTIMESTAMP - NUMTODSINTERVAL(60,'SECOND'),
                 ENABLED     = 'Y',
                 UPDATED_AT  = SYSTIMESTAMP
            WHEN NOT MATCHED THEN INSERT
                 (NAME, DESCRIPTION, CRON_EXPR, NEXT_DUE_AT, ENABLED, CREATED_AT, UPDATED_AT)
            VALUES ('demo', 'demo job', '*/5 * * * * ?', SYSTIMESTAMP - NUMTODSINTERVAL(60,'SECOND'), 'Y', SYSTIMESTAMP, SYSTIMESTAMP)
        """);
        Long jobId = jdbc.queryForObject("SELECT ID FROM TB_JOB WHERE NAME='demo'", Long.class);

        tx.required(() -> {
            Instant now = Instant.now();
            // indegree: t1=0, t2=1 (t1 -> t2)
            tasks.upsert(new Task(null, jobId, "t1", "handler.t1", null, null, "first", 0, now, now));
            tasks.upsert(new Task(null, jobId, "t2", "handler.t2", null, null, "second", 1, now, now));

            var t1 = tasks.findByJobAndName(jobId, "t1").orElseThrow();
            var t2 = tasks.findByJobAndName(jobId, "t2").orElseThrow();
            deps.add(t1.id(), t2.id()); // 멱등
            return null;
        });
    }

    private void startFakeWorker() {
        workerPool.submit(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // 1) 오케스트레이터 tick (due job → jobRun 생성 → taskRuns 초기화/승격)
                    orchestrator.tick(Duration.ofSeconds(5), Duration.ofSeconds(30), 5);

                    // 2) RUNNING 만들기: READY를 클레임
                    tx.required(() -> {
                        taskDispatchService.claimAndStartUpTo(1, Duration.ofSeconds(60));
                        // 단순하게: READY 중 하나를 RUNNING으로 바꾸기 위해 TaskRunRepository.claimReady 등을 쓰고
                        // (여기서는 서비스가 아닌 레포로 직접 접근 필요할 수 있음)
                        // 이미 여러분의 TaskDispatchService가 있다면 그걸 써도 됨.
                        return null;
                    });

                    // 3) RUNNING → DONE: 현재 RUNNING들을 완료 처리 (가짜 작업자)
                    tx.required(() -> {
                        List<TaskRun> running = findRunning();
                        for (TaskRun tr : running) {
                            // 실제론 핸들러 실행 후 성공이면 DONE, 실패면 retryWithBackoff
                            // 여기선 테스트 목적이므로 바로 DONE
                            taskRuns.markDone(tr.id());
                        }
                        return null;
                    });

                    // maintenance로 승격/만료 복구
                    // (스케줄러에도 있지만 테스트 속도 위해 주기적으로 한번 더)
                    Thread.sleep(300); // 너무 바쁘게 돌지 않게
                }
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private List<TaskRun> findRunning() throws Exception {
        // 간단 쿼리로 RUNNING 목록 얻기
        return tx.required(() -> {
            return jdbc.query("""
                SELECT * FROM TB_TASK_RUN WHERE STATUS='RUNNING' ORDER BY ID
            """, (rs, rowNum) -> new TaskRun(
                    rs.getLong("ID"),
                    rs.getLong("JOB_RUN_ID"),
                    rs.getLong("TASK_ID"),
                    TaskRun.Status.from(rs.getString("STATUS")),
                    rs.getLong("ATTEMPT"),
                    rs.getInt("PRE_CNT"),
                    rs.getInt("DONE_CNT"),
                    rs.getInt("WORKER_ID"),
                    rs.getTimestamp("AVAILABLE_AT") == null ? null : rs.getTimestamp("AVAILABLE_AT").toInstant(),
                    rs.getTimestamp("LEASE_UNTIL") == null ? null : rs.getTimestamp("LEASE_UNTIL").toInstant(),
                    rs.getTimestamp("STARTED_AT") == null ? null : rs.getTimestamp("STARTED_AT").toInstant(),
                    rs.getTimestamp("FINISHED_AT") == null ? null : rs.getTimestamp("FINISHED_AT").toInstant(),
                    rs.getTimestamp("CREATED_AT").toInstant(),
                    rs.getTimestamp("UPDATED_AT").toInstant(),
                    rs.getString("LAST_ERROR")
            ));
        });
    }
}
