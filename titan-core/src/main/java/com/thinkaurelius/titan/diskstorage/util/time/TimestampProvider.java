package com.thinkaurelius.titan.diskstorage.util.time;

import com.thinkaurelius.titan.core.attribute.Duration;

import java.util.concurrent.TimeUnit;

/**
 * System time interface that abstracts time units, resolution, and measurements of time.
 */
public interface TimestampProvider {

    /**
     * Returns the current time based on this timestamp provider
     * as a {@link Timepoint}.
     *
     * @return
     */
    public Timepoint getTime();

    /**
     * Returns the given time as a {@link Timepoint} based off of this timestamp provider
     * @param sinceEpoch
     * @param unit
     * @return
     */
    public Timepoint getTime(long sinceEpoch, TimeUnit unit);

    /**
     * Return the units of {@link #getTime()}. This method's return value must
     * be constant over at least the life of the object implementing this
     * interface.
     *
     * @return this instance's time unit
     */
    public TimeUnit getUnit();

    /**
     * Block until the current time as returned by {@link #getTime()} is greater
     * than the given timepoint.
     *
     * @param futureTime The time to sleep past
     *
     * @return the current time in the same units as the {@code unit} argument
     * @throws InterruptedException
     *             if externally interrupted
     */
    public Timepoint sleepPast(Timepoint futureTime) throws InterruptedException;

    /**
     * Sleep for the given duration of time.
     *
     * @param duration
     * @throws InterruptedException
     */
    public void sleepFor(Duration duration) throws InterruptedException;

    /**
     * Returns a {@link Timer} based on this timestamp provider
     *
     * @return
     */
    public Timer getTimer();

}
