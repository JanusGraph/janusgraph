package com.thinkaurelius.titan.core.time;

import java.util.concurrent.TimeUnit;

/**
 * A length of time without any specific relationship to a calendar or standard
 * clock.
 */
public interface Duration extends Comparable<Duration> {
    public long getLength(TimeUnit unit);
    public boolean isZeroLength();
    public Duration sub(Duration subtrahend);
    public Duration add(Duration addend);
    public Duration mult(double multiplier);
}
