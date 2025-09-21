package net.majide.core.service;

import net.majide.core.model.Job;
import net.majide.core.model.JobRun;
import net.majide.core.spi.*;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public final class JobTickService {
    private final JobRepository jobs;
    private final JobRunRepository jobRuns;
    private final TaskGraphService taskGraph;
    private final TxRunner tx;
    private final Clock clock;
    private final CronCalculator cron;

    public JobTickService(JobRepository jobs,
                          JobRunRepository jobRuns,
                          TaskGraphService taskGraph,
                          TxRunner tx, Clock clock, CronCalculator cron) {
        this.jobs = jobs;
        this.jobRuns = jobRuns;
        this.taskGraph = taskGraph;
        this.tx = tx;
        this.clock = clock;
        this.cron = cron;
    }

    /** due인 Job 하나를 선점 → JobRun 멱등 생성 → Task 그래프를 준비 → 커서 전진 */
    public void tickOnce(Duration lease) throws Exception {
        tx.requiresNew(() -> {
            var opt = jobs.claimDueJob(lease, "dispatcher"); // FOR UPDATE SKIP LOCKED 내부
            if (opt.isEmpty()) return null;

            Job job = opt.get();
            // 1) runKey 계산 (예: 'yyyy-MM-dd'T'HH:mm' 같은 정책)
            String runKey = computeRunKey(clock.now(), job);

            // 2) 멱등 JobRun upsert (CREATED 또는 유지)
            JobRun run = jobRuns.upsert(job.id(), runKey, JobRun.Status.CREATED);

            // 3) 태스크 그래프 준비: BLOCKED/READY 세팅, preCnt/doneCnt 초기화
            taskGraph.prepareFor(job.id(), run.id());

            // 4) 커서 전진(nextDueAt)
            Instant next = cron.next(job.nextDueAt(), job.cronExpr(), ZoneId.systemDefault());
            jobs.advanceCursor(job.id(), next);

            return null;
        });
    }

    private String computeRunKey(Instant now, Job job) {
        // 팀 규칙에 맞는 포맷으로. 예: cron 슬롯 시작시각을 키로.
        return now.toString(); // 예시. 실제로는 Truncated/rounded 시각 or 슬롯 키 계산.
    }
}
