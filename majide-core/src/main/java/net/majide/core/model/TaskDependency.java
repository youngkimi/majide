package net.majide.core.model;


import java.time.Instant;

public record TaskDependency (
        Long preTaskId,
        Long postTaskId,
        Instant createdAt
) {}