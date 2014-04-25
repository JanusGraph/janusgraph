package com.thinkaurelius.titan.core.time;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.google.common.base.Preconditions;

@DefaultSerializer(SimpleDurationKryoSerializer.class)
public class SimpleDuration implements Duration {

    private static final Logger log =
            LoggerFactory.getLogger(SimpleDuration.class);

    private final long length;
    private final TimeUnit unit;

    public SimpleDuration(long length, TimeUnit unit) {
        this.length = length;
        this.unit = unit;
        Preconditions.checkArgument(0 <= this.length, "Time durations must be non-negative");
        Preconditions.checkNotNull(this.unit);
    }

    @Override
    public int compareTo(Duration o) {
        /*
         * Don't do this:
         *
         * return (int)(o.getLength(unit) - getLength(unit));
         *
         * 2^31 ns = 2.14 seconds and 2^31 us = 36 minutes. The narrowing cast
         * from long to integer is practically guaranteed to cause failures at
         * either nanosecond resolution (where almost everything will fail) or
         * microsecond resolution (where the failures would be more insidious;
         * perhaps lock expiration malfunctioning).
         *
         * The following implementation is ugly, but unlike subtraction-based
         * implementations, it is affected by neither arithmetic overflow
         * (because it does no arithmetic) nor loss of precision from
         * long-to-integer casts (because it does not cast).
         */
        final long mine = getLength(unit);
        final long theirs = o.getLength(unit);
        if (mine < theirs) {
            return -1;
        } else if (theirs < mine) {
            return 1;
        }
        return 0;
    }

    @Override
    public long getLength(TimeUnit target) {
        return target.convert(length, unit);
    }

    @Override
    public Duration sub(Duration subtrahend) {
        long result = getLength(unit) - subtrahend.getLength(unit);
        if (0 > result) {
            result = 0;
        }
        return new SimpleDuration(result, unit);
    }

    @Override
    public Duration add(Duration addend) {
        return new SimpleDuration(getLength(unit) + addend.getLength(unit), unit);
    }

    @Override
    public boolean isZeroLength() {
        return length == 0;
    }

    public TimeUnit getNativeUnit() {
        return unit;
    }

    @Override
    public Duration mult(double multiplier) {
        Preconditions.checkArgument(0 <= multiplier, "Time multiplier %d is negative", multiplier);
        long newLength = (long)(length * multiplier);
        if (newLength < length) {
            /*
             * Trying to be clever with unit conversions to avoid overflow is a
             * waste of effort. A duration long enough to trigger this condition
             * probably reflects a bug in the caller, even with a very
             * fine-resolution TimeUnit. For example, 2^63 microseconds is more
             * than 292 thousand years.
             */
            log.warn("Duration overflow detected: {} * {} exceeds representable range of long; using Long.MAX_VALUE instead", length, multiplier);
            newLength = Long.MAX_VALUE;
        }
        return new SimpleDuration(newLength, unit);
    }

    @Override
    public int hashCode() {
        return 31 * 17 + (int)(length ^ (length));
        // We ignore TimeUnits here, but pay attention to it in equals
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleDuration other = (SimpleDuration) obj;

        return other.getLength(unit) == length;
    }

    @Override
    public String toString() {
        return String.format("Duration[%d %s]", length, Durations.abbreviate(unit));
    }

}