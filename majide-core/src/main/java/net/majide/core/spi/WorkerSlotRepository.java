package net.majide.core.spi;

import net.majide.core.model.WorkerSlot;

import java.time.Duration;
import java.util.Optional;

public interface WorkerSlotRepository {
    Optional<WorkerSlot> leaseFreeSlot(String token, Duration lease) throws Exception; // 임차

    void heartbeat(int workerId, String token, Duration lease) throws Exception;       // 연장

    void release(int workerId, String token) throws Exception;                         // 반납

    /** lease_until 지난 슬롯 회수(instance_token/lease_until NULL) */
    int reclaimExpired() throws Exception;                                             // 만료 회수(옵션)

    Optional<WorkerSlot> findById(int workerId) throws Exception;
}