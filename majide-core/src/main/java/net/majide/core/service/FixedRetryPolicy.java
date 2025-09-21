package net.majide.core.service;

import java.time.Duration;

final class FixedRetryPolicy implements RetryPolicy {
    private final Duration backoff;
    public FixedRetryPolicy(Duration backoff) { this.backoff = backoff; }
    @Override public Duration nextBackoff(long attempt) { return backoff; }
}
