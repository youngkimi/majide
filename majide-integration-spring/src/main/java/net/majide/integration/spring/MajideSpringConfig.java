package net.majide.integration.spring;

import net.majide.adapter.jdbc.repo.*;
import net.majide.core.spi.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class MajideSpringConfig {

    // TxRunner (Spring)
    @Bean
    public TxRunner txRunner(org.springframework.transaction.PlatformTransactionManager tm,
                             javax.sql.DataSource ds) {
        return new net.majide.integration.spring.tx.SpringTxRunner(tm, ds);
    }
    // Repository 구현 등록 (adapter-jdbc 재사용)
    @Bean public JobRepository jobRepository(DataSource ds) { return new JdbcJobRepository(ds); }
    @Bean public JobRunRepository jobRunRepository(DataSource ds) { return new JdbcJobRunRepository(ds); }
    @Bean public TaskRepository taskRepository(DataSource ds) { return new JdbcTaskRepository(ds); }
    @Bean public TaskDependencyRepository taskDependencyRepository(DataSource ds) { return new JdbcTaskDependencyRepository(ds); }
    @Bean public TaskRunRepository taskRunRepository(DataSource ds) { return new JdbcTaskRunRepository(ds); }
    @Bean public WorkerSlotRepository workerSlotRepository(DataSource ds) { return new JdbcWorkerSlotRepository(ds); }

    // Clock/CronCalculator는 앱에서 주입하거나, 기본 구현 빈을 여기서 제공해도 됨.
    // 예: 기본 Clock
    @Bean public Clock systemClock() { return java.time.Instant::now; }

    // 예: CronCalculator는 코어에서 SPI만 선언했으니, 구현체를 별도 모듈/앱에서 주입하거나
    // 여기에서 cron-utils 기반 구현을 제공하는 클래스를 만들어 빈으로 등록해도 된다.
}
