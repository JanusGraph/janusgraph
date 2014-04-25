package com.thinkaurelius.titan.core.time;

import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.google.common.base.Preconditions;

/**
 * An instant in time
 */
@DefaultSerializer(TimepointKryoSerializer.class)
public class Timepoint implements Comparable<Timepoint> {


    private final long ts;
    private final TimeUnit unit;

    /**
     * Number of time units elapsed between the UNIX Epoch and this instant.
     * {@code ts} may by any value in the representable range of {@code long}.
     *
     * @param ts
     *            time units since UNIX Epoch
     * @param unit
     *            units of {@code ts} parameter
     */
    public Timepoint(final long ts, final TimeUnit unit) {
        this.ts = ts;
        this.unit = unit;
        Preconditions.checkNotNull(this.unit);
    }

    public long getTime(TimeUnit returnUnit) {
        return returnUnit.convert(ts, unit);
    }

    public Timepoint add(Duration addend) {
        // Use this object's unit in returned object
        // TODO check for and warn on loss of precision
        return new Timepoint(ts + addend.getLength(unit), unit);
    }

    public Timepoint sub(Duration subtrahend) {
        return new Timepoint(ts - subtrahend.getLength(unit), unit);
    }

    public TimeUnit getNativeUnit() {
        return unit;
    }

    @Override
    public int compareTo(Timepoint other) {
        final long theirs = other.getTime(unit);
        if (ts < theirs) {
            return -1;
        } else if (theirs < ts) {
            return 1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        return 31 * 17 + (int)(ts ^ (ts));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Timepoint other = (Timepoint) obj;

        return other.getTime(unit) == ts;
    }

    @Override
    public String toString() {
        return String.format("Timepoint[%d %s]", ts, Durations.abbreviate(unit));
    }
}
