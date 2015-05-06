package com.thinkaurelius.titan.diskstorage.util.time;



import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;

/**
 * System time interface that abstracts time units, resolution, and measurements of time.
 */
public interface TimestampProvider {

    /**
     * Returns the current time based on this timestamp provider
     * as a {@link Instant}.
     *
     * @return
     */
    public Instant getTime();

    /**
     * Returns the given time as a {@link Instant} based off of this timestamp providers units
     * @param sinceEpoch
     * @return
     */
    public Instant getTime(long sinceEpoch);

    /**
     * Return the units of {@link #getTime()}. This method's return value must
     * be constant over at least the life of the object implementing this
     * interface.
     *
     * @return this instance's time unit
     */
    public ChronoUnit getUnit();

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
    public Instant sleepPast(Instant futureTime) throws InterruptedException;

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


    /**
     * Returns the scalar value for this instant given the configured time unit
     * @param timestamp
     * @return
     */
    long getTime(Instant timestamp);
}
