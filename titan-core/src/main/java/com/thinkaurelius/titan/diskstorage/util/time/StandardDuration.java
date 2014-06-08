package com.thinkaurelius.titan.diskstorage.util.time;

import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.core.attribute.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class StandardDuration implements Duration {

    private static final Logger log =
            LoggerFactory.getLogger(StandardDuration.class);

    private final long length;
    private final TimeUnit unit;

    private StandardDuration() {
        //For Kryo
        length=0;
        unit = null;
    }

    public StandardDuration(long length, TimeUnit unit) {
        this.length = length;
        this.unit = unit;
        Preconditions.checkArgument(0 <= this.length, "Time durations must be non-negative");
        Preconditions.checkNotNull(this.unit);
    }

    @Override
    public int compareTo(Duration o) {
        return Durations.compare(length,unit,o.getLength(o.getNativeUnit()),o.getNativeUnit());
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
        return new StandardDuration(result, unit);
    }

    @Override
    public Duration add(Duration addend) {
        return new StandardDuration(getLength(unit) + addend.getLength(unit), unit);
    }

    @Override
    public boolean isZeroLength() {
        return length == 0;
    }

    @Override
    public TimeUnit getNativeUnit() {
        return unit;
    }

    @Override
    public Duration multiply(double multiplier) {
        Preconditions.checkArgument(0 <= multiplier, "Time multiplier %d is negative", multiplier);
        if (isZeroLength() || multiplier==0) return ZeroDuration.INSTANCE;
        else if (multiplier==1.0) return this;

        double actualLength = length * multiplier;
        long newLength = Math.round(actualLength);

        if (multiplier>=1.0 && newLength < length) { //Detect overflow
            /*
             * Trying to be clever with unit conversions to avoid overflow is a
             * waste of effort. A duration long enough to trigger this condition
             * probably reflects a bug in the caller, even with a very
             * fine-resolution TimeUnit. For example, 2^63 microseconds is more
             * than 292 thousand years.
             */
            log.warn("Duration overflow detected: {} * {} exceeds representable range of long; using Long.MAX_VALUE instead", length, multiplier);
            return new StandardDuration(Long.MAX_VALUE,unit);
        } else if (Math.abs(newLength-actualLength)/actualLength>MULTIPLY_PRECISION) { //Detect loss of precision
            /*
            We are going directly to the most precise-unit to make things easier.
            This could trigger a subsequent overflow, however, in practice this if conditions
            should only be triggered for values of around 100 days or less (days being the longest TimeUnit)
            which is less than 10^17 nano seconds (and hence fits into a long).
             */
            return new StandardDuration(TimeUnit.NANOSECONDS.convert(length,unit),TimeUnit.NANOSECONDS).multiply(multiplier);
        } else {
            return new StandardDuration(newLength, unit);
        }
    }

    private static final double MULTIPLY_PRECISION = 0.01;

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
        StandardDuration other = (StandardDuration) obj;

        return other.getLength(unit) == length;
    }

    @Override
    public String toString() {
        return String.format("Duration[%d %s]", length, Durations.abbreviate(unit));
    }

}