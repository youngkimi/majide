package net.majide.core.spi;

import java.time.Instant;

public interface Clock {
    Instant now();
}