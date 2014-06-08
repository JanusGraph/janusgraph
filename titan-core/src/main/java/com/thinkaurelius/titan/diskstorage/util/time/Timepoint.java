package com.thinkaurelius.titan.diskstorage.util.time;

import com.thinkaurelius.titan.core.attribute.Duration;

import java.util.concurrent.TimeUnit;

/**
 * An instant in time backed by a {@link TimestampProvider}
 */
public interface Timepoint extends Comparable<Timepoint> {

    /**
     * Returns this timepoint as the elapsed time since UNIX epoch in the given time unit.
     *
     * @param targetUnit
     * @return
     */
    public long getTimestamp(TimeUnit targetUnit);

    /**
     * Returns a new Timepoint which is the given duration later in time.
     * @param addend
     * @return
     */
    public Timepoint add(Duration addend);

    /**
     * Returns a new Timepoint that is the given duration earlier in time.
     * @param subtrahend
     * @return
     */
    public Timepoint sub(Duration subtrahend);

    /**
     * Returns the native time unit of this time point as given by the backing {@link TimestampProvider}.
     * </p>
     * Equivalent to {@code getProvider().getUnit()}.
     *
     * @return
     */
    public TimeUnit getNativeUnit();

    /**
     * Returns the elapsed time since UNIX epoch of this time point in the native unit.
     * </p>
     * Equivalent to {@code getTimestamp(getNativeUnit())}
     * @return
     */
    public long getNativeTimestamp();

    /**
     * Returns the backing {@link TimestampProvider}.
     * @return
     */
    public TimestampProvider getProvider();

    /**
     * Causes this thread to sleep past this time point.
     *
     * @return
     * @throws InterruptedException
     */
    public Timepoint sleepPast() throws InterruptedException;

}
