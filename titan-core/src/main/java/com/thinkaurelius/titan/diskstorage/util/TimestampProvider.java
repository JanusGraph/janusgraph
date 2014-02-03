package com.thinkaurelius.titan.diskstorage.util;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

/**
 * System time interface that abstracts time units and resolution.
 */
public interface TimestampProvider {

    /**
     * Return the current time in the units specified by
     * {@link TimestampProvider#getUnit()} on this instance.
     *
     * @return current time
     */
    public long getTime();

    /**
     * Return the units reported by {@link #getTime()}. This method's return
     * value must be constant over at least the life of the object implementing
     * this interface. Clients of this interface may call this method once and
     * assume that the return value never changes thereafter.
     *
     * @return this instance's time unit
     */
    public TimeUnit getUnit();

    /**
     * Block until the current time returned by {@link #getTime()} is greater
     * than or equal to the argument. If the parameter {@code unit} and
     * {@link #getUnit()} are different, then this method internally converts
     * between them as necessary.
     *
     * @param time
     *            the time to reach or pass
     * @param unit
     *            units associated with the time parameter
     * @param log
     *            the logger to use (if necessary)
     * @return the current time in the same units as the {@code unit} argument
     * @throws InterruptedException
     *             if interrupted
     */
    public long sleepUntil(long time, TimeUnit unit, Logger log)
            throws InterruptedException;

    public long sleepUntil(long time, TimeUnit unit)
            throws InterruptedException;

}
