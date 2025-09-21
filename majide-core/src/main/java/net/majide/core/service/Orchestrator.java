package net.majide.core.service;

import java.time.Duration;

public final class Orchestrator {
    private final JobTickService jobTick;
    private final TaskDispatchService taskDispatch;

    public Orchestrator(JobTickService jobTick, TaskDispatchService taskDispatch) {
        this.jobTick = jobTick;
        this.taskDispatch = taskDispatch;
    }


    // TODO Job Run Status 변화. Fetch Ready Job Run. Fetch Readt Task Run... ...
    /** 한 번의 틱: (1) Job 커서 전진/JobRun 준비 (2) 태스크 디스패치  */
    public void tick(Duration jobLease, Duration taskLease, int maxTaskClaimsPerTick) throws Exception {
        jobTick.tickOnce(jobLease);
        taskDispatch.claimAndStartUpTo(maxTaskClaimsPerTick, taskLease);
    }
}
