package net.majide.integration.spring.cron;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** cron-utils 기반 슬롯 계산기 (Guava 없이 LRU 캐시) */
public final class CronSlotPlanner {
    private static final CronParser PARSER =
            new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    // 간단 LRU(최대 256개). TTL이 꼭 필요하면 Caffeine 사용(아래 2안 참고)
    private static final Map<String, ExecutionTime> CACHE = new LruMap<>(256);

    public static SlotInfo compute(String cronExpr, ZoneId zone, Instant now) {
        Objects.requireNonNull(cronExpr); Objects.requireNonNull(zone); Objects.requireNonNull(now);

        final ExecutionTime et = CACHE.computeIfAbsent(cronExpr, expr ->
                ExecutionTime.forCron(PARSER.parse(expr)));

        var base = now.atZone(zone);
        var next = et.nextExecution(base).orElseThrow(
                () -> new IllegalStateException("No next execution for ["+cronExpr+"] at "+base));
        var slotStart = et.lastExecution(next).orElseGet(() ->
                et.lastExecution(base).orElseThrow(
                        () -> new IllegalStateException("No last execution for ["+cronExpr+"] at "+base)));

        return new SlotInfo(slotStart.toInstant(), next.toInstant());
    }

    public static void invalidate(String expr) { synchronized (CACHE) { CACHE.remove(expr); } }
    public static void invalidateAll() { synchronized (CACHE) { CACHE.clear(); } }

    public record SlotInfo(Instant slotStartUtc, Instant nextUtc) {
        public String key(String ns) { return ns + ":" + slotStartUtc; }
    }

    // --- 내부 LRU ---
    private static final class LruMap<K,V> extends LinkedHashMap<K,V> {
        private final int max;
        LruMap(int max) { super(16, 0.75f, true); this.max = max; }
        @Override protected boolean removeEldestEntry(Map.Entry<K,V> eldest) { return size() > max; }
    }
}
