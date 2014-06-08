package com.thinkaurelius.titan.diskstorage.util.time;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.Duration;

import java.util.concurrent.TimeUnit;

/**
 * An instant in time
 */
public class StandardTimepoint implements Timepoint {


    private final long sinceEpoch;
    private final TimestampProvider provider;

    private StandardTimepoint() {
        //For kryo
        sinceEpoch=0;
        provider=null;
    }

    /**
     * Number of time units elapsed between the UNIX Epoch and this instant.
     * {@code sinceEpoch} may by any value in the representable range of {@code long}.
     *
     * @param sinceEpoch
     *            time units since UNIX Epoch
     * @param provider
     *            the underlying {@link TimestampProvider} that the timestamp is based off of
     */
    public StandardTimepoint(final long sinceEpoch, final TimestampProvider provider) {
        Preconditions.checkArgument(provider!=null);
        this.sinceEpoch = sinceEpoch;
        this.provider = provider;
    }

    @Override
    public long getTimestamp(TimeUnit returnUnit) {
        return returnUnit.convert(sinceEpoch, getNativeUnit());
    }

    @Override
    public StandardTimepoint add(Duration addend) {
        // Use this object's unit in returned object
        // TODO check for and warn on loss of precision
        return new StandardTimepoint(sinceEpoch + addend.getLength(getNativeUnit()), provider);
    }

    @Override
    public StandardTimepoint sub(Duration subtrahend) {
        return new StandardTimepoint(sinceEpoch - subtrahend.getLength(getNativeUnit()), provider);
    }

    @Override
    public TimeUnit getNativeUnit() {
        return provider.getUnit();
    }

    @Override
    public long getNativeTimestamp() {
        return sinceEpoch;
    }

    @Override
    public TimestampProvider getProvider() {
        return provider;
    }

    @Override
    public Timepoint sleepPast() throws InterruptedException {
        return provider.sleepPast(this);
    }

    @Override
    public int compareTo(Timepoint other) {
        final long theirs = other.getTimestamp(getNativeUnit());
        if (sinceEpoch < theirs) {
            return -1;
        } else if (theirs < sinceEpoch) {
            return 1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        return 31 * 17 + (int)(sinceEpoch ^ (sinceEpoch));
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

        return other.getTimestamp(getNativeUnit()) == sinceEpoch;
    }

    @Override
    public String toString() {
        return String.format("Timepoint[%d %s]", sinceEpoch, Durations.abbreviate(getNativeUnit()));
    }
}
