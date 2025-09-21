package net.majide.core.spi;

import net.majide.core.model.Job;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public interface JobRepository {
    Optional<Job> claimDueJob(Duration lease, String owner) throws Exception;  // FOR UPDATE SKIP LOCKED
    void advanceCursor(long jobId, Instant nextDueAt) throws Exception;        // 커서 전진 + lease 해제
    Optional<Job> findById(long id) throws Exception;
    Optional<Job> findByName(String name) throws Exception;
    void save(Job job) throws Exception; // 생성/업데이트(필요 시)

    Job upsert(String name,
               String description,
               String cronExpr,
               Instant nextDueAt,
               boolean enabled) throws Exception;

    default Job upsert(String name,
                       String description,
                       String cronExpr,
                       Instant nextDueAt) throws Exception {
        return upsert(name, description, cronExpr, nextDueAt, true);
    }
}