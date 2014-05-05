package com.thinkaurelius.titan.util.time;

import java.util.concurrent.TimeUnit;

/**
 * A zero-length {@link Duration} singleton.
 *
 */
public class ZeroDuration implements Duration {

    public static Duration INSTANCE = new ZeroDuration();

    private ZeroDuration() { }

    @Override
    public int compareTo(Duration o) {
        if (o.getLength(TimeUnit.NANOSECONDS) == 0) {
            return 0;
        } else {
            return -1;
        }
    }

    @Override
    public long getLength(TimeUnit unit) {
        return 0;
    }

    public TimeUnit getNativeUnit() {
        return TimeUnit.NANOSECONDS;
    }

    @Override
    public Duration sub(Duration subtrahend) {
        return this;
    }

    @Override
    public Duration add(Duration addend) {
        return addend;
    }

    @Override
    public boolean isZeroLength() {
        return true;
    }

    @Override
    public Duration multiply(double multiplier) {
        return this;
    }
}
