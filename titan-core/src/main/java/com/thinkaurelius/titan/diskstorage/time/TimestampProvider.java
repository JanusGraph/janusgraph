package com.thinkaurelius.titan.diskstorage.time;

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
     * Convenience method for {@code getUnit().convert}.
     *
     * @param sourceDuration
     * @param sourceUnit
     * @return
     */
    public long convert(long sourceDuration, TimeUnit sourceUnit);

    /**
     * Returns a (shorthand) string representation of the time unit
     */
    public String getUnitName();

    /**
     * Block until the current time as returned by {@link #getTime()} is greater
     * than the provided epochTime, where it is assumed that epochTime is given
     * in the same time unit as {@link #getUnit()}.
     *
     * @param epochTime
     *            the timestamp to exceed before returning
     *
     * @return the current time in the same units {@link #getUnit()}
     * @throws InterruptedException
     *             if externally interrupted
     */
    public long sleepPast(long epochTime)
            throws InterruptedException;

    /**
     * Sleeps for the given interval of time where the time unit is {@link #getUnit()}
     *
     * @param duration
     * @throws InterruptedException
     */
    public void sleepFor(long duration) throws InterruptedException;

}
