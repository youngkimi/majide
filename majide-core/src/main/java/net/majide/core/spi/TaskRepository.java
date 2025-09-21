package net.majide.core.spi;

import net.majide.core.model.Task;

import java.util.List;
import java.util.Optional;

public interface TaskRepository {
    Optional<Task> findByJobAndName(long jobId, String taskName) throws Exception;
    List<Task> findAllByJob(long jobId) throws Exception;

    /** 멱등 upsert: (JOB_ID, TASK_NAME) 기반 */
    void upsert(Task task) throws Exception;
}