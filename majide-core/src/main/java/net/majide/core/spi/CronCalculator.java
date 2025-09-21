package net.majide.core.spi;

import java.time.Instant;
import java.time.ZoneId;

public interface CronCalculator {
    Instant next(Instant from, String cronExpr, ZoneId zone);
}