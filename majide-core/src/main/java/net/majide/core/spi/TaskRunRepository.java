package net.majide.core.spi;

import net.majide.core.model.TaskRun;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface TaskRunRepository {
    /** READY + available_at<=now 중 하나를 선점(RUNNING 전환, lease_until 설정) */
    Optional<TaskRun> claimReady(Duration lease, String workerToken) throws Exception;

    /** 하트비트: lease 연장 */
    void heartbeat(long taskRunId, Duration lease) throws Exception;

    void markDone(long taskRunId) throws Exception;

    /** 실패 후 백오프 재시도: READY로 되돌리고 available_at = now + backoff, attempt++ */
    void retryWithBackoff(long taskRunId, Duration backoff, String lastError) throws Exception;

    /** 선행 완료 반영: doneCnt 증가 → preCnt 도달 시 READY 승격(available_at=now) */
    void incrementDoneCount(long taskRunId) throws Exception;

    Optional<TaskRun> findById(long id) throws Exception;
    List<TaskRun> findAllByJobRun(long jobRunId) throws Exception;

    /**  멱등 생성/초기화: preCnt=given, doneCnt=0, status=BLOCKED, available_at=NULL, attempt=1 */
    void createOrReset(long jobRunId, long taskId, int preCnt, TaskRun.Status status, Instant available) throws Exception;

    // ★ Maintenance용 (추가)
    /** lease_until 만료된 RUNNING을 READY로 되돌리고 available_at=now, attempt+1, last_error 세팅 */
    int recoverExpiredLeases(Duration backoff, String reason) throws Exception;

    /** BLOCKED인데 doneCnt >= preCnt인 태스크를 READY로 승격(available_at=now) */
    int promoteUnblockedToReady() throws Exception;

    /** 오래된 DONE/FAILED TaskRun 정리(옵션: 보관일수 지난 것 soft-delete or archive flag 설정 등) */
    int archiveFinishedOlderThan(Instant threshold) throws Exception;

    /** READY인데 available_at 과도하게 과거/미래로 틀어진 이상치 보정 (옵션) */
    int normalizeReadyAvailability(Instant min, Instant max) throws Exception;
}