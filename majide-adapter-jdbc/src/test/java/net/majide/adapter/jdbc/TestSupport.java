package net.majide.adapter.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Optional;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestSupport {
    protected static DataSource ds;
    protected static OracleContainer oracle;

    @BeforeAll
    void setupDb() {
        String url = System.getenv("ORACLE_JDBC_URL");
        String user = System.getenv("ORACLE_USERNAME");
        String pass = System.getenv("ORACLE_PASSWORD");

        if (url == null || url.isBlank()) {
            // Testcontainers Oracle XE (gvenzl/oracle-xe)
            oracle = new OracleContainer(DockerImageName.parse("gvenzl/oracle-xe:21-slim"))
                    .withStartupTimeout(Duration.ofMinutes(5));
            oracle.start();

            url = oracle.getJdbcUrl();
            user = Optional.ofNullable(oracle.getUsername()).orElse("system");
            pass = Optional.ofNullable(oracle.getPassword()).orElse("oracle");
        }

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setUsername(user);
        cfg.setPassword(pass);
        cfg.setMaximumPoolSize(6);
        cfg.setMinimumIdle(1);
        cfg.setConnectionTimeout(30_000);
        cfg.setIdleTimeout(60_000);
        cfg.setDriverClassName("oracle.jdbc.OracleDriver");
        ds = new HikariDataSource(cfg);

        // Flyway (테스트 전용 위치)
        Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration/oracle")
                .baselineOnMigrate(true)
                .load()
                .migrate();
    }

    @AfterAll
    void cleanup() {
        if (ds instanceof HikariDataSource h) h.close();
        if (oracle != null) oracle.stop();
    }
}