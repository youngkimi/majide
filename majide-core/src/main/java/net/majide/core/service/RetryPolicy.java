package net.majide.core.service;

import java.time.Duration;

public interface RetryPolicy {
    Duration nextBackoff(long attempt);

    /** 고정 백오프 정책 */
    static RetryPolicy fixed(Duration backoff) {
        return attempt -> backoff;
    }
}
