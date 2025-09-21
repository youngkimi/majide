package net.majide.core.model;

import java.time.Instant;

public record TaskRun(
        Long id,
        Long jobRunId,
        Long taskId,
        Status status,
        Long attempt,
        Integer preCnt,
        Integer doneCnt,
        Integer workerId,
        Instant availableAt,
        Instant leaseUntil,
        Instant startedAt,
        Instant finishedAt,
        Instant createdAt,
        Instant updatedAt,
        String lastError
) {
    public enum Status {
        BLOCKED, READY, RUNNING, DONE, FAILED, SKIPPED, CANCELLED, EXPIRED, UNKNOWN;

        public static Status from(String s) {
            if (s == null) return UNKNOWN;
            try { return Status.valueOf(s.toUpperCase()); } catch (IllegalArgumentException e) { return UNKNOWN; }
        }
        public String code() { return name(); }
    }
}
