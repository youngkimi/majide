package net.majide.core.spi;

import net.majide.core.model.JobRun;

import java.util.Optional;

public interface JobRunRepository {
    JobRun upsert(long jobId, String runKey, JobRun.Status initialStatus) throws Exception;

    Optional<JobRun> findByJobAndRunKey(long jobId, String runKey) throws Exception;

    void markStarted(long jobRunId) throws Exception;
    void markFinished(long jobRunId, boolean success) throws Exception; // DONE/FAILED 중 택
}