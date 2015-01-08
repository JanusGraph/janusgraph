package com.thinkaurelius.titan.core.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.util.time.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Timestamp implements Comparable<Timestamp> {

    private static final Logger log =
            LoggerFactory.getLogger(Timestamp.class);

    private long sinceEpoch;
    private TimeUnit unit;

    private Timestamp() {} //For Kryo serialization

    public Timestamp(long sinceEpoch, TimeUnit unit) {
        Preconditions.checkNotNull(unit);
        this.sinceEpoch = sinceEpoch;
        this.unit = unit;
    }

    @Override
    public int compareTo(Timestamp o) {
        return Durations.compare(sinceEpoch, unit, o.sinceEpoch(o.getNativeUnit()), o.getNativeUnit());
    }

    /**
     * Returns the length of time since UNIX epoch in the given {@link java.util.concurrent.TimeUnit}.
     *
     * @param target
     * @return
     */
    public long sinceEpoch(TimeUnit target) {
        return target.convert(sinceEpoch, unit);
    }

    /**
     * Returns the native unit used by this Timestamp. The actual time is specified in this unit of time.
     * </p>
     * @return
     */
    public TimeUnit getNativeUnit() {
        return unit;
    }

    @Override
    public int hashCode() {
        return 31 * 17 + (int)(sinceEpoch ^ (sinceEpoch));
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
        Timestamp other = (Timestamp) obj;

        return other.sinceEpoch(unit) == sinceEpoch;
    }

    @Override
    public String toString() {
        return String.format("Timestamp[%d %s]", sinceEpoch, Durations.abbreviate(unit));
    }

}