package com.thinkaurelius.titan.diskstorage.time;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        public long getTime() {
            return System.nanoTime() - t0NanoTime + t0NanosSinceEpoch;
        }

        @Override
        public TimeUnit getUnit() {
            return TimeUnit.NANOSECONDS;
        }

        @Override
        public String getUnitName() {
            return "ns";
        }
    },

    MICRO {
        @Override
        public long getTime() {
            return System.currentTimeMillis() * 1000L;
        }

        @Override
        public TimeUnit getUnit() {
            return TimeUnit.MICROSECONDS;
        }

        @Override
        public String getUnitName() {
            return "us";
        }
    },

    MILLI {
        @Override
        public long getTime() {
            return System.currentTimeMillis();
        }

        @Override
        public TimeUnit getUnit() {
            return TimeUnit.MILLISECONDS;
        }
        @Override
        public String getUnitName() {
            return "ms";
        }
    };

    private static final Logger log =
            LoggerFactory.getLogger(Timestamps.class);

    @Override
    public long sleepPast(final long time) throws InterruptedException {

        // All long variables are times in parameter unit

        long now;

        while ((now = getTime()) <= time) {
            long delta = time - now;
            if (0L == delta)
                delta = 1L;

            //Make sure we sleep for at least one millisecond
            delta = Math.max(delta,getUnit().convert(1,TimeUnit.MILLISECONDS));

            /*
             * TimeUnit#sleep(long) internally preserves the nanoseconds parts
             * of the argument, if applicable, and passes both milliseconds and
             * nanoseconds parts into Thread#sleep(long, long)
             */
            if (log.isDebugEnabled()) {
                log.debug("Sleeping: now={} targettime={} delta={} (unit={})",
                        new Object[] { now, time, delta, getUnit() });
            }
            getUnit().sleep(delta);
        }

        assert time < now;

        return now;
    }

    @Override
    public void sleepFor(long duration) throws InterruptedException {
        Preconditions.checkArgument(duration>=0,"Sleep time must be positive: %s",duration);
        getUnit().sleep(duration);
    }

    @Override
    public long convert(long sourceDuration, TimeUnit sourceUnit) {
        return getUnit().convert(sourceDuration,sourceUnit);
    }

    @Override
    public String toString() {
        return "Timestamps[" + getUnit() + "]";
    }

    // ========================= ACCESS ==================

    static TimestampProvider SYSTEM_TIMESTAMP = MICRO;

    public static final TimestampProvider SYSTEM() {
        return SYSTEM_TIMESTAMP;
    }





}
