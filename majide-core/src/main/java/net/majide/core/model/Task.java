package net.majide.core.model;

import java.time.Instant;

public record Task(
        Long id,
        Long jobId,
        String name,
        String handlerKey,
        String classFqn,
        String methodName,
        String description,
        Integer indegree,
        Instant createdAt,
        Instant updatedAt
) {}
