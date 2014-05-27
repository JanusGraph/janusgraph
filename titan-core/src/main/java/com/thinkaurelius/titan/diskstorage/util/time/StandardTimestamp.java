package com.thinkaurelius.titan.diskstorage.util.time;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.attribute.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class StandardTimestamp implements Timestamp {

    private static final Logger log =
            LoggerFactory.getLogger(StandardTimestamp.class);

    private final long sinceEpoch;
    private final TimeUnit unit;

    public StandardTimestamp(long sinceEpoch, TimeUnit unit) {
        Preconditions.checkNotNull(unit);
        this.sinceEpoch = sinceEpoch;
        this.unit = unit;
    }

    @Override
    public int compareTo(Timestamp o) {
        return Durations.compare(sinceEpoch,unit,o.sinceEpoch(o.getNativeUnit()),o.getNativeUnit());
    }

    @Override
    public long sinceEpoch(TimeUnit target) {
        return target.convert(sinceEpoch, unit);
    }

    @Override
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
        StandardTimestamp other = (StandardTimestamp) obj;

        return other.sinceEpoch(unit) == sinceEpoch;
    }

    @Override
    public String toString() {
        return String.format("Timestamp[%d %s]", sinceEpoch, Durations.abbreviate(unit));
    }

}