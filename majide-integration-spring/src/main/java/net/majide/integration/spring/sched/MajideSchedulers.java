package net.majide.integration.spring.sched;

import net.majide.core.maintenance.MaintenanceService;
import net.majide.core.service.Orchestrator;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;

public class MajideSchedulers {
    private final Orchestrator orchestrator;
    private final MaintenanceService maintenance;

    private Duration jobLease = Duration.ofSeconds(5);
    private Duration taskLease = Duration.ofSeconds(30);
    private int maxClaims = 10;
    private Duration maintBackoff = Duration.ofSeconds(10);
    private Duration finishedTtl = Duration.ofDays(30);

    public MajideSchedulers(Orchestrator orchestrator, MaintenanceService maintenance) {
        this.orchestrator = orchestrator;
        this.maintenance = maintenance;
    }

    @Scheduled(fixedDelayString = "${majide.scheduler.tick-delay-ms:3000}")
    public void tick() throws Exception {
        orchestrator.tick(jobLease, taskLease, maxClaims);
    }

    @Scheduled(fixedDelayString = "${majide.scheduler.maintenance-delay-ms:10000}")
    public void maintenance() throws Exception {
        maintenance.runOnce(maintBackoff, finishedTtl,
                Duration.ofMinutes(30), Duration.ofMinutes(30));
    }
    public void setJobLease(Duration jobLease) {
        this.jobLease = jobLease;
    }

    public void setTaskLease(Duration taskLease) {
        this.taskLease = taskLease;
    }

    public void setMaxClaims(int maxClaims) {
        this.maxClaims = maxClaims;
    }

    public void setMaintBackoff(Duration maintBackoff) {
        this.maintBackoff = maintBackoff;
    }

    public void setFinishedTtl(Duration finishedTtl) {
        this.finishedTtl = finishedTtl;
    }
}
