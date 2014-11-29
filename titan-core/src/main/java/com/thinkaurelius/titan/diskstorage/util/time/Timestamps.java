package com.thinkaurelius.titan.diskstorage.util.time;

import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.core.attribute.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementations of {@link TimestampProvider} for different resolutions of time:
 * <ul>
 *     <li>NANO: nano-second time resolution based on System.nanoTime using a base-time established
 *     by System.currentTimeMillis(). The exact resolution depends on the particular JVM and host machine.</li>
 *     <li>MICRO: micro-second time which is actually at milli-second resolution.</li>
 *     <li>MILLI: milli-second time resolution</li>
 * </ul>
 */
public enum Timestamps implements TimestampProvider {
    NANO {

        // This is the value of System.nanoTime() at startup
        private final long t0NanoTime;

        /* This is the value of System.currentTimeMillis() at
         * startup times a million (i.e. CTM in ns)
         */
        private final long t0NanosSinceEpoch;

        // Initialize the t0 variables
        {

            /*
             * This is a crude attempt to establish a correspondence
             * between System.currentTimeMillis() and System.nanoTime().
             *
             * It's susceptible to errors up to +/- 999 us due to the
             * limited accuracy of System.currentTimeMillis()
             * versus that of System.nanoTime(), with an average
             * error of about -0.5 ms.
             *
             * In addition, it's susceptible to arbitrarily large
             * error if the scheduler decides to sleep this thread
             * in between the following time calls.
             *
             * One mitigation for both errors could be to wrap
             * this logic in a loop and combine the timing information
             * from multiple passes into the final t0 values.
             */
            final long t0ms = System.currentTimeMillis();
            final long t0ns = System.nanoTime();

            t0NanosSinceEpoch = t0ms * 1000L * 1000L;
            t0NanoTime = t0ns;
            LoggerFactory.getLogger(Timestamps.class)
                    .trace("Initialized nanotime. currentTimeMillis component: {} ms. nanoTime component: {} ns.",
                            t0ms, t0ns);
        }

        /**
         * This returns the approximate number of nanoseconds
         * elapsed since the UNIX Epoch.  The least significant
         * bit is overridden to 1 or 0 depending on whether
         * setLSB is true or false (respectively).
         * <p/>
         * This timestamp rolls over about every 2^63 ns, or
         * just over 292 years.  The first rollover starting
         * from the UNIX Epoch would be sometime in 2262.
         *
         * @return a timestamp as described above
         */
        @Override
        public Timepoint getTime() {
            return new StandardTimepoint(getTimeInternal(), NANO);
        }

        @Override
        public TimeUnit getUnit() {
            return TimeUnit.NANOSECONDS;
        }

        private final long getTimeInternal() {
            return System.nanoTime() - t0NanoTime + t0NanosSinceEpoch;
        }
    },

    MICRO {
        @Override
        public Timepoint getTime() {
            return new StandardTimepoint(System.currentTimeMillis() * 1000L, MICRO);
        }

        @Override
        public TimeUnit getUnit() {
            return TimeUnit.MICROSECONDS;
        }
    },

    MILLI {
        @Override
        public Timepoint getTime() {
            return new StandardTimepoint(System.currentTimeMillis(), MILLI);
        }

        @Override
        public TimeUnit getUnit() {
            return TimeUnit.MILLISECONDS;
        }
    };

    private static final Logger log =
            LoggerFactory.getLogger(Timestamps.class);

    @Override
    public Timepoint sleepPast(Timepoint futureTime) throws InterruptedException {

        Timepoint now;

        TimeUnit unit = getUnit();

        /*
         * Distributed storage managers that rely on timestamps play with the
         * least significant bit in timestamp longs, turning it on or off to
         * ensure that deletions are logically ordered before additions within a
         * single batch mutation. This is not a problem at microsecond
         * resolution because we pretendulate microsecond resolution by
         * multiplying currentTimeMillis by 1000, so the LSB can vary freely.
         * It's also not a problem with nanosecond resolution because the
         * resolution is just too fine, relative to how long a mutation takes,
         * for it to matter in practice. But it can lead to corruption at
         * millisecond resolution (and does, in testing).
         */
        if (unit.equals(TimeUnit.MILLISECONDS))
            futureTime = futureTime.add(new StandardDuration(1L, TimeUnit.MILLISECONDS));

        while ((now = getTime()).compareTo(futureTime) <= 0) {
            long delta = futureTime.getTimestamp(unit) - now.getTimestamp(unit);
            if (0L == delta)
                delta = 1L;
            /*
             * TimeUnit#sleep(long) internally preserves the nanoseconds parts
             * of the argument, if applicable, and passes both milliseconds and
             * nanoseconds parts into Thread#sleep(long, long)
             */
            if (log.isTraceEnabled()) {
                log.trace("Sleeping: now={} targettime={} delta={} {}",
                        new Object[] { now, futureTime, delta, unit });
            }
            unit.sleep(delta);
        }

        return now;
    }

    @Override
    public void sleepFor(Duration duration) throws InterruptedException {
        if (duration.isZeroLength()) return;

        TimeUnit unit = duration.getNativeUnit();
        unit.sleep(duration.getLength(unit));
    }

    @Override
    public Timer getTimer() {
        return new Timer(this);
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public Timepoint getTime(long sinceEpoch, TimeUnit unit) {
        return new StandardTimepoint(getUnit().convert(sinceEpoch,unit),this);
    }
}
