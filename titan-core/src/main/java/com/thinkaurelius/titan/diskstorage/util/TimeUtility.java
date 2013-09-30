package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Utility methods for measuring fine time intervals
 */
public enum TimeUtility implements TimestampProvider {
    INSTANCE;

    private static final Logger log =
            LoggerFactory.getLogger(TimeUtility.class);

    // Initialize the t0 variables
    {

		/*
         * This is a crude attempt to establish a correspondence
		 * between System.currentTimeMillis() and System.nanoTime().
		 * 
		 * It's susceptible to errors up to -999 us due to the
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
    }

    // This is the value of System.nanoTime() at startup
    private final long t0NanoTime;

    /* This is the value of System.currentTimeMillis() at
     * startup times a million (i.e. CTM in ns)
     */
    private final long t0NanosSinceEpoch;

    // TODO this does not belong here
    private static final long MILLION = 1000L * 1000L;

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
     * @param setLSB should the smallest bit in the
     *               returned value be one?
     * @return a timestamp as described above
     */
//    @Override
//    public long getApproxNSSinceEpoch(final boolean setLSB) {
//        final long nanosSinceEpoch = System.nanoTime() - t0NanoTime + t0NanosSinceEpoch;
//        final long ts = ((nanosSinceEpoch) & 0xFFFFFFFFFFFFFFFEL) + (setLSB ? 1L : 0L);
//        return ts;
//    }
    @Override
    public long getApproxNSSinceEpoch() {
        return (System.nanoTime() - t0NanoTime + t0NanosSinceEpoch);
    }

    /**
     * Sleep until {@link #getApproxNSSinceEpoch(false)} returns a number
     * greater than or equal to the argument. This method loops internally to
     * handle spurious wakeup.
     *
     * @param untilNS the timestamp to meet or exceed before returning (unless
     *                interrupted first)
     */
    @Override
    public long sleepUntil(final long untilNS) throws InterruptedException {
        long nowNS;

        for (nowNS = getApproxNSSinceEpoch();
             nowNS < untilNS;
             nowNS = getApproxNSSinceEpoch()) {

            // Convert time delta from nano to millis, rounding up
            final long deltaNS = untilNS - nowNS;
            final long deltaMS;
            if (0 != deltaNS % 1000000) {
                deltaMS = deltaNS / MILLION + 1;
            } else {
                deltaMS = deltaNS / MILLION;
            }

            if (0 >= deltaMS) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipped sleep: target wakeup time {} ms already past current time {} ms (delta {})",
                            new Object[]{
                                    TimeUnit.MILLISECONDS.convert(untilNS, TimeUnit.NANOSECONDS),
                                    TimeUnit.MILLISECONDS.convert(nowNS, TimeUnit.NANOSECONDS),
                                    deltaMS
                            }
                    );
                }
                return nowNS;
            }

            if (log.isDebugEnabled()) {
                log.debug("Sleeping: target wakeup time {} ms, current time {} ms, duration {} ms",
                        new Object[]{
                                TimeUnit.MILLISECONDS.convert(untilNS, TimeUnit.NANOSECONDS),
                                TimeUnit.MILLISECONDS.convert(nowNS, TimeUnit.NANOSECONDS),
                                deltaMS
                        }
                );
            }

            Thread.sleep(deltaMS);
        }

        return nowNS;
    }

    public final void sleepUntil(long untilTimeMillis, Logger log) throws StorageException {
        long now;

        while (true) {
            now = System.currentTimeMillis();

            if (now > untilTimeMillis) {
                break;
            }

            long delta = untilTimeMillis - now + 1;

            assert 0 <= delta;

            try {
                if (log != null) log.debug("About to sleep for {} ms", delta);
                Thread.sleep(delta);
            } catch (InterruptedException e) {
                throw new PermanentLockingException("Interrupted while waiting", e);
            }
        }
    }


}
