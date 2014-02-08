package com.thinkaurelius.titan.diskstorage.util;

import java.util.concurrent.TimeUnit;

/**
 * System time interface that abstracts time units and resolution.
 */
public interface TimestampProvider {

    /**
     * Return the current time since the UNIX epoch in the units specified by
     * {@link TimestampProvider#getUnit()} on this instance.
     *
     * @return current time
     */
    public long getTime();

    /**
     * Return the units of {@link #getTime()}. This method's return value must
     * be constant over at least the life of the object implementing this
     * interface. Clients of this interface may call this method once and assume
     * that the return value never changes thereafter.
     *
     * @return this instance's time unit
     */
    public TimeUnit getUnit();

    /**
     * Block until the current time as returned by {@link #getTime()} is greater
     * than the parameters. If the parameter {@code unit} and {@link #getUnit()}
     * are different, then this method internally converts between them as
     * necessary for comparison.
     *
     * @param epochTime
     *            the timestamp to exceed before returning
     * @param unit
     *            units associated with the time parameter
     *
     * @return the current time in the same units as the {@code unit} argument
     * @throws InterruptedException
     *             if externally interrupted
     */
    public long sleepPast(long epochTime, TimeUnit unit)
            throws InterruptedException;

}
