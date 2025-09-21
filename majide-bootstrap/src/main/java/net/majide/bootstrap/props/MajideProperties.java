package net.majide.bootstrap.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties("majide")
public class MajideProperties {
    private Catalog catalog = new Catalog();
    private String zone = "UTC";
    private Scheduler scheduler = new Scheduler();

    public Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public static class Catalog {
        private boolean enabled = true;
        private List<JobDef> jobs = new ArrayList<>(); // ← 가변

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public List<JobDef> getJobs() {
            return jobs;
        }

        public void setJobs(List<JobDef> jobs) {
            this.jobs = jobs;
        }
    }
    public static class JobDef {
        private String name;
        private String description;
        private String cronExpr;
        private List<TaskDef> tasks = new ArrayList<>(); // ← 가변

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getCronExpr() {
            return cronExpr;
        }

        public void setCronExpr(String cronExpr) {
            this.cronExpr = cronExpr;
        }

        public List<TaskDef> getTasks() {
            return tasks;
        }

        public void setTasks(List<TaskDef> tasks) {
            this.tasks = tasks;
        }

        @Override
        public String toString() {
            return "JobDef{" +
                    "name='" + name + '\'' +
                    ", description='" + description + '\'' +
                    ", cronExpr='" + cronExpr + '\'' +
                    ", tasks=" + tasks +
                    '}';
        }
    }
    public static class TaskDef {
        private String name;
        private String handler;
        private List<String> dependsOn = new ArrayList<>();      // ← 가변
        private Map<String, String> attrs = new LinkedHashMap<>(); // ← 가변

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getHandler() {
            return handler;
        }

        public void setHandler(String handler) {
            this.handler = handler;
        }

        public List<String> getDependsOn() {
            return dependsOn;
        }

        public void setDependsOn(List<String> dependsOn) {
            this.dependsOn = dependsOn;
        }

        public Map<String, String> getAttrs() {
            return attrs;
        }

        public void setAttrs(Map<String, String> attrs) {
            this.attrs = attrs;
        }
    }

    public static class Scheduler {
        private boolean enabled = true;
        private long tickDelayMs = 3000;
        private long maintenanceDelayMs = 10000;
        private Duration jobLease = Duration.ofSeconds(5);
        private Duration taskLease = Duration.ofSeconds(30);
        private int maxClaims = 10;
        private Duration retryBackoff = Duration.ofSeconds(10);
        private Duration finishedTtl = Duration.ofDays(30);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getTickDelayMs() {
            return tickDelayMs;
        }

        public void setTickDelayMs(long tickDelayMs) {
            this.tickDelayMs = tickDelayMs;
        }

        public long getMaintenanceDelayMs() {
            return maintenanceDelayMs;
        }

        public void setMaintenanceDelayMs(long maintenanceDelayMs) {
            this.maintenanceDelayMs = maintenanceDelayMs;
        }

        public Duration getJobLease() {
            return jobLease;
        }

        public void setJobLease(Duration jobLease) {
            this.jobLease = jobLease;
        }

        public Duration getTaskLease() {
            return taskLease;
        }

        public void setTaskLease(Duration taskLease) {
            this.taskLease = taskLease;
        }

        public int getMaxClaims() {
            return maxClaims;
        }

        public void setMaxClaims(int maxClaims) {
            this.maxClaims = maxClaims;
        }

        public Duration getRetryBackoff() {
            return retryBackoff;
        }

        public void setRetryBackoff(Duration retryBackoff) {
            this.retryBackoff = retryBackoff;
        }

        public Duration getFinishedTtl() {
            return finishedTtl;
        }

        public void setFinishedTtl(Duration finishedTtl) {
            this.finishedTtl = finishedTtl;
        }
    }
}