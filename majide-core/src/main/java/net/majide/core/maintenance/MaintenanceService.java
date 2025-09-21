package net.majide.core.maintenance;

import net.majide.core.spi.Clock;
import net.majide.core.spi.TaskRunRepository;
import net.majide.core.spi.TxRunner;
import net.majide.core.spi.WorkerSlotRepository;

import java.time.Duration;
import java.time.Instant;

public final class MaintenanceService {
    private final WorkerSlotRepository workers;
    private final TaskRunRepository taskRuns;
    private final TxRunner tx;
    private final Clock clock;

    public static final String DEFAULT_EXPIRED_REASON = "lease expired — recovered by maintenance";

    public MaintenanceService(WorkerSlotRepository workers,
                              TaskRunRepository taskRuns,
                              TxRunner tx,
                              Clock clock) {
        this.workers = workers;
        this.taskRuns = taskRuns;
        this.tx = tx;
        this.clock = clock;
    }

    /**
     * 주기 점검 메인 루틴.
     * - 워커 슬롯 회수
     * - 만료된 RUNNING 재노출(READY)
     * - 선행 충족된 BLOCKED 승격
     * - 오래된 완료건 아카이브(선택)
     * - READY 이상치 보정(선택)
     */
    public MaintenanceReport runOnce(Duration defaultBackoff,
                                     Duration finishedTtl,
                                     Duration readyWindowPast,
                                     Duration readyWindowFuture) throws Exception {
        Instant now = clock.now();
        MaintenanceReport r = new MaintenanceReport();

        // 1) 좀비 워커 회수
        r.reclaimedWorkers = tx.required(() -> workers.reclaimExpired());

        // 2) RUNNING lease 만료 복구 → READY(+backoff, attempt++)
        r.recoveredTasks = tx.required(() ->
                taskRuns.recoverExpiredLeases(defaultBackoff, DEFAULT_EXPIRED_REASON));

        // 3) BLOCKED인데 doneCnt>=preCnt → READY 승격
        r.promotedToReady = tx.required(taskRuns::promoteUnblockedToReady);

        // 4) 오래된 완료건 아카이브 (TTL 지난 것)
        if (finishedTtl != null && !finishedTtl.isZero() && !finishedTtl.isNegative()) {
            Instant threshold = now.minus(finishedTtl);
            r.archivedFinished = tx.required(() ->
                    taskRuns.archiveFinishedOlderThan(threshold));
        }

        // 5) READY available_at 이상치 보정 (옵션)
        if (readyWindowPast != null && readyWindowFuture != null) {
            Instant min = now.minus(readyWindowPast);
            Instant max = now.plus(readyWindowFuture);
            r.normalizedReady = tx.required(() ->
                    taskRuns.normalizeReadyAvailability(min, max));
        }

        r.timestamp = now;
        return r;
    }

    /** 간단 리포트 DTO */
    public static final class MaintenanceReport {
        public Instant timestamp;
        public int reclaimedWorkers;
        public int recoveredTasks;
        public int promotedToReady;
        public int archivedFinished;
        public int normalizedReady;

        @Override public String toString() {
            return "MaintenanceReport{" +
                    "timestamp=" + timestamp +
                    ", reclaimedWorkers=" + reclaimedWorkers +
                    ", recoveredTasks=" + recoveredTasks +
                    ", promotedToReady=" + promotedToReady +
                    ", archivedFinished=" + archivedFinished +
                    ", normalizedReady=" + normalizedReady +
                    '}';
        }
    }
}
