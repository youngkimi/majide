package net.majide.core.service;

import net.majide.core.model.Task;
import net.majide.core.model.TaskRun;
import net.majide.core.spi.*;

import java.util.List;

public final class TaskGraphService {
    private final TaskRepository tasks;
    private final TaskDependencyRepository deps;
    private final TaskRunRepository taskRuns;
    private final TxRunner tx;
    private final Clock clock;

    public TaskGraphService(TaskRepository tasks,
                            TaskDependencyRepository deps,
                            TaskRunRepository taskRuns,
                            TxRunner tx,
                            Clock clock) {
        this.tasks = tasks;
        this.deps = deps;
        this.taskRuns = taskRuns;
        this.tx = tx;
        this.clock = clock;
    }

    /** 새 JobRun에 대해 TaskRun들을 생성/초기화: 선행 있으면 BLOCKED, 없으면 READY */
    public void prepareFor(long jobId, long jobRunId) throws Exception {
        tx.required(() -> {
            List<Task> taskList = tasks.findAllByJob(jobId);
            for (Task t : taskList) {
                int pre = t.indegree() == null ? 0 : t.indegree();
                // (여기선 단순화: TaskRun upsert는 TaskRunRepository 내부 정책으로 처리 가능하다고 가정)
                if (pre > 0) {
                    // BLOCKED, preCnt = pre, doneCnt=0
                    // AVAILABLE_AT null
                    taskRuns.createOrReset(jobRunId, t.id(), pre, TaskRun.Status.BLOCKED, null);
                } else {
                    // READY, available_at = now
                    taskRuns.createOrReset(jobRunId, t.id(), pre, TaskRun.Status.READY, clock.now());
                }
            }
            return null;
        });
    }

    /** 선행 완료 반영: doneCnt 증가, preCnt 도달 시 READY 승격(available_at=now) */
    public void onPredecessorDone(long taskRunId) throws Exception {
        tx.required(() -> { taskRuns.incrementDoneCount(taskRunId); return null; });
    }
}
