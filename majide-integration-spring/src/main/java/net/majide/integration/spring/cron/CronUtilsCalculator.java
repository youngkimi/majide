package net.majide.integration.spring.cron;

import net.majide.core.spi.CronCalculator;
import java.time.Instant;
import java.time.ZoneId;

/** 코어 SPI 구현체 (next만 필요할 때) */
public final class CronUtilsCalculator implements CronCalculator {
    @Override
    public Instant next(Instant from, String cronExpr, ZoneId zone) {
        return CronSlotPlanner.compute(cronExpr, zone, from).nextUtc();
    }
}