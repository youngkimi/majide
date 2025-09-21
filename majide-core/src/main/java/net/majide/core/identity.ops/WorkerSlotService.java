package net.majide.core.identity.ops;

import net.majide.core.model.WorkerSlot;
import net.majide.core.spi.WorkerSlotRepository;

import java.time.Duration;
import java.util.Optional;

/** 워커 슬롯 임차/하트비트/반납 오퍼레이션 */
public final class WorkerSlotService {
    private final WorkerSlotRepository repo;

    public WorkerSlotService(WorkerSlotRepository repo) {
        this.repo = repo;
    }

    /** 가용 슬롯 하나 임차 (token은 caller에서 생성/주입: hostname:pid:uuid 등) */
    public Optional<WorkerSlot> leaseOne(String token, Duration lease) throws Exception {
        return repo.leaseFreeSlot(token, lease);
    }

    /** 하트비트(리스 연장) */
    public void heartbeat(int workerId, String token, Duration lease) throws Exception {
        repo.heartbeat(workerId, token, lease);
    }

    /** 반납 */
    public void release(int workerId, String token) throws Exception {
        repo.release(workerId, token);
    }

    /** 만료 슬롯 회수 (선택: 스케줄러에서 주기 호출) */
    public int reclaimExpired() throws Exception {
        return repo.reclaimExpired();
    }
}
