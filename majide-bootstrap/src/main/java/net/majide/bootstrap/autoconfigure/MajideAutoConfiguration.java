package net.majide.bootstrap.autoconfigure;

import net.majide.bootstrap.catalog.CatalogRegistrar;
import net.majide.bootstrap.props.MajideProperties;
import net.majide.core.maintenance.MaintenanceService;
import net.majide.core.service.*;
import net.majide.core.spi.*;
import net.majide.integration.spring.MajideSpringConfig;
import net.majide.integration.spring.cron.CronSlotPlanner;
import net.majide.integration.spring.sched.MajideSchedulers;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.ZoneId;
import java.util.stream.Collectors;

@AutoConfiguration
@EnableConfigurationProperties(MajideProperties.class)
@Import(MajideSpringConfig.class) // integration-spring: repos/tx/clock wiring
public class MajideAutoConfiguration {

    // --- SPI 기본 구현(없으면) 제공 ---

    @Bean
    @ConditionalOnMissingBean(CronCalculator.class)
    public CronCalculator cronCalculator() {
        // integration-spring의 CronSlotPlanner를 이용해 next 계산
        return (from, expr, zone) -> CronSlotPlanner.compute(expr, zone, from).nextUtc();
    }

    // --- 코어 서비스 조립 ---

    @Bean
    @ConditionalOnMissingBean
    public TaskGraphService taskGraph(TaskRepository tasks,
                                      TaskDependencyRepository deps,
                                      TaskRunRepository taskRuns,
                                      TxRunner tx,
                                      Clock clock) {
        return new TaskGraphService(tasks, deps, taskRuns, tx, clock);
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskDispatchService taskDispatch(TaskRunRepository taskRuns, TxRunner tx, MajideProperties props) {
        Duration backoff = props.getScheduler().getRetryBackoff();
        return new TaskDispatchService(taskRuns, tx, RetryPolicy.fixed(backoff));
    }

    @Bean
    @ConditionalOnMissingBean
    public Orchestrator orchestrator(JobTickService jobTick, TaskDispatchService dispatch) {
        return new Orchestrator(jobTick, dispatch);
    }

    @Bean
    @ConditionalOnMissingBean
    public JobTickService jobTick(JobRepository jobs,
                                  JobRunRepository jobRuns,
                                  TaskGraphService graph,
                                  TxRunner tx,
                                  Clock clock,
                                  CronCalculator cron) {
        return new JobTickService(jobs, jobRuns, graph, tx, clock, cron);
    }

    @Bean
    @ConditionalOnMissingBean
    public MaintenanceService maintenance(WorkerSlotRepository workers,
                                          TaskRunRepository taskRuns,
                                          TxRunner tx,
                                          Clock clock) {
        return new MaintenanceService(workers, taskRuns, tx, clock);
    }

    // --- 스케줄러 등록 (프로퍼티로 주기 제어) ---
//    @Bean
//    @ConditionalOnProperty(prefix = "majide.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
//    public MajideSchedulers majideSchedulers(Orchestrator orchestrator,
//                                             MaintenanceService maintenance) {
//        return new MajideSchedulers(orchestrator, maintenance);
//    }

    @Bean
    @ConditionalOnProperty(prefix = "majide.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
    public MajideSchedulers majideSchedulers(Orchestrator orchestrator,
                                             MaintenanceService maintenance,
                                             MajideProperties props) {
        var s = new MajideSchedulers(orchestrator, maintenance);

        // @Scheduled의 딜레이는 아래 YAML 키(majide.tick.delay-ms / majide.maintenance.delay-ms)에서 읽힘.
        // 나머지 파라미터만 세터로 주입
        s.setJobLease(props.getScheduler().getJobLease());
        s.setTaskLease(props.getScheduler().getTaskLease());
        s.setMaxClaims(props.getScheduler().getMaxClaims());
        s.setMaintBackoff(props.getScheduler().getRetryBackoff()); // 이름만 다름: retryBackoff -> maintBackoff
        s.setFinishedTtl(props.getScheduler().getFinishedTtl());
        return s;
    }

    @Bean
    public CatalogRegistrar catalogRegistrar(JobRepository jobs,
                                             TaskRepository tasks,
                                             TaskDependencyRepository deps,
                                             TxRunner tx,
                                             MajideProperties props) {
        ZoneId zone = ZoneId.of(props.getZone());
        return new CatalogRegistrar(jobs, tasks, deps, tx, zone);
    }

    @Bean
    @ConditionalOnProperty(prefix = "majide.catalog", name = "enabled", havingValue = "true", matchIfMissing = true)
    public ApplicationRunner catalogRunner(CatalogRegistrar registrar,
                                           MajideProperties props) {
        System.out.println("[Majide] catalogRunner start: enabled=" + props.getCatalog().isEnabled());
        System.out.println("[Majide] catalog: \n" + props.getCatalog().getJobs().stream().map(MajideProperties.JobDef::toString).collect(Collectors.joining("\n")));
        return args -> registrar.register(props.getCatalog());
    }
}
