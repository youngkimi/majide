package net.majide.core.spi;

import java.util.List;

public interface TaskDependencyRepository {
    void add(long preTaskId, long postTaskId) throws Exception;
    void remove(long preTaskId, long postTaskId) throws Exception;
    List<Long> findPredecessors(long taskId) throws Exception;
    List<Long> findSuccessors(long taskId) throws Exception;
}