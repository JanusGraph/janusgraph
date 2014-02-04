package com.thinkaurelius.titan.diskstorage.util;

import java.util.concurrent.TimeUnit;

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
        public long sleepUntil(long time, final TimeUnit unit, final Logger log) throws InterruptedException {
            return super.sleepUntil(TimeUnit.MILLISECONDS.convert(time, unit) + 1, TimeUnit.MILLISECONDS, log);
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
    };

    private static final Logger log =
            LoggerFactory.getLogger(Timestamps.class);

    @Override
    public long sleepUntil(final long time, final TimeUnit unit, final Logger log) throws InterruptedException {

        // All long variables are times in parameter unit

        long now;

        while ((now = unit.convert(getTime(), getUnit())) <= time) {
            final long delta = time - now;
            /*
             * TimeUnit#sleep(long) internally preserves the nanoseconds parts
             * of the argument, if applicable, and passes both milliseconds and
             * nanoseconds parts into Thread#sleep(long, long)
             */
            if (log.isDebugEnabled()) {
                log.debug("Sleeping: now={} targettime={} delta={} (unit={})",
                        new Object[] { now, time, delta, unit });
            }
            unit.sleep(delta);
        }

        return now;
    }

    @Override
    public long sleepUntil(final long time, final TimeUnit unit) throws InterruptedException {
        return sleepUntil(time, unit, log);
    }

    @Override
    public String toString() {
        return "Timestamps[" + getUnit() + "]";
    }
}
