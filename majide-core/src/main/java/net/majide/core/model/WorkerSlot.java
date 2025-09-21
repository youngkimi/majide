package net.majide.core.model;

import java.time.Instant;

public record WorkerSlot(
        Integer workerId,        // 사전 할당된 0..N
        String instanceToken,    // 이 프로세스 식별자(UUID 등), null = 미임차
        Instant leaseUntil,      // 임차(가시성) 만료 시각
        Instant heartbeatAt,     // 마지막 하트비트
        Instant createdAt,
        Instant updatedAt
) {
    public boolean leased() {
        return instanceToken != null && leaseUntil != null;
    }
}
