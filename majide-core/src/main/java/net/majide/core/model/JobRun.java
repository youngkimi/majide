package net.majide.core.model;

import java.time.Instant;

public record JobRun (
        Long id,
        Long jobId,
        String runKey,      // 예: "2025-09-19T16:00"
        Status status,      // CREATED/RUNNING/DONE/FAILED/CANCELLED/EXPIRED
        Instant createdAt,
        Instant updatedAt,
        Instant deadlineAt,
        Instant startedAt,
        Instant finishedAt
) {
    public enum Status {
        CREATED, RUNNING, DONE, FAILED, CANCELLED, EXPIRED, UNKNOWN;

        public static Status from(String s) {
            if (s == null) return UNKNOWN;
            try { return Status.valueOf(s.toUpperCase()); } catch (IllegalArgumentException e) { return UNKNOWN; }
        }
        public String code() { return name(); }
    }
}

