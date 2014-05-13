package com.thinkaurelius.titan.core.attribute;

import java.util.concurrent.TimeUnit;

/**
 * A length of time without any specific relationship to a calendar or standard
 * clock.
 */
public interface Duration extends Comparable<Duration> {

    /**
     * Returns the length of this duration in the given {@link TimeUnit}.
     *
     * @param unit
     * @return
     */
    public long getLength(TimeUnit unit);

    /**
     * Whether this duration is of zero length.
     * @return
     */
    public boolean isZeroLength();

    /**
     * Returns the native unit used by this duration. The actual time length is specified in this unit of time.
     * </p>
     * @return
     */
    public TimeUnit getNativeUnit();

    /**
     * Returns a new duration that equals the length of this duration minus the length of the given duration
     * in the unit of this duration.
     *
     * @param subtrahend
     * @return
     */
    public Duration sub(Duration subtrahend);

    /**
     * Returns a new duration that equals the combined length of this and the given duration in the
     * unit of this duration.
     *
     * @param addend
     * @return
     */
    public Duration add(Duration addend);

    /**
     * Multiplies the length of this duration by the given multiplier. The multiplier must be a non-negative number.
     *
     * @param multiplier
     * @return
     */
    public Duration multiply(double multiplier);


}
