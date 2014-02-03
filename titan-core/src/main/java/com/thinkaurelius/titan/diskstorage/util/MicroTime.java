package com.thinkaurelius.titan.diskstorage.util;

import java.util.concurrent.TimeUnit;

/**
 * Milliseconds since UNIX Epoch multiplied by 1000.
 * <p>
 * Though this implementation returns time in microseconds, it's implemented by
 * multiplying {@link System#currentTimeMillis()} by a thousand. The final three
 * digits in the values it returns are always zero, so it does not have any
 * effective microsecond-level resolution. This approach mimics Cassandra's
 * server-side timestamp generation.
 */
public class MicroTime extends AbstractTimestampProvider {

    public static final MicroTime INSTANCE = new MicroTime();

    private MicroTime() { }

    @Override
    public long getTime() {
        return System.currentTimeMillis() * 1000L;
    }

    @Override
    public TimeUnit getUnit() {
        return TimeUnit.MICROSECONDS;
    }
}
