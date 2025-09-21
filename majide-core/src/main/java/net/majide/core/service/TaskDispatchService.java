package net.majide.core.service;

import net.majide.core.model.TaskRun;
import net.majide.core.spi.TaskRunRepository;
import net.majide.core.spi.TxRunner;

import java.time.Duration;

public final class TaskDispatchService {
    private final TaskRunRepository taskRuns;
    private final TxRunner tx;
    private final RetryPolicy retry;

    public TaskDispatchService(TaskRunRepository taskRuns, TxRunner tx, RetryPolicy retry) {
        this.taskRuns = taskRuns; this.tx = tx; this.retry = retry;
    }

    /** READY를 최대 N개까지 클레임하여 RUNNING으로 전환 (워크 플로우 시작) */
    public int claimAndStartUpTo(int maxCount, Duration lease) throws Exception {
        int claimed = 0;
        for (; claimed < maxCount; claimed++) {
            var picked = tx.requiresNew(() -> taskRuns.claimReady(lease, "worker")); // worker 토큰은 바깥에서 주입해도 됨
            if (picked.isEmpty()) break;

            TaskRun tr = picked.get();
            // 여기서 실제 실행 트리거(큐에 enqueue 등)는 외부 어댑터에서 처리.
            // 코어는 상태 전이만 책임.
        }
        return claimed;
    }

    /** 하트비트 (실행 중인 태스크의 lease 연장) */
    public void heartbeat(long taskRunId, Duration lease) throws Exception {
        tx.required(() -> { taskRuns.heartbeat(taskRunId, lease); return null; });
    }

    /** 성공 완료 */
    public void markDone(long taskRunId) throws Exception {
        tx.required(() -> { taskRuns.markDone(taskRunId); return null; });
    }

    /** 실패 → 백오프로 READY 재전환 */
    public void failAndRetry(long taskRunId, long attempt, String error) throws Exception {
        tx.required(() -> {
            var backoff = retry.nextBackoff(attempt);
            taskRuns.retryWithBackoff(taskRunId, backoff, error);
            return null;
        });
    }
}
