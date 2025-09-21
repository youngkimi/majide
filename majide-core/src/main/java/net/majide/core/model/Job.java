package net.majide.core.model;

import java.time.Instant;

public record Job(
        Long id,
        String name,
        String description,
        String cronExpr,
        Instant nextDueAt,
        Instant leaseUntil,
        boolean enabled,
        Instant createdAt,
        Instant updatedAt
) {
    public static Job ofNew(String name, String description, String cronExpr, Instant nextDueAt) {
        return new Job(null, name, description, cronExpr, nextDueAt, null, true, null, null);
    }
}