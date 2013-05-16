package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import org.slf4j.Logger;

/**
 * Utility methods for measuring fine time intervals
 */
public class TimeUtility {

    // Initialize the t0 variables
    static {

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
    private static final long t0NanoTime;

    /* This is the value of System.currentTimeMillis() at
     * startup times a million (i.e. CTM in ns)
     */
    private static final long t0NanosSinceEpoch;

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
    public static long getApproxNSSinceEpoch(final boolean setLSB) {
        final long nanosSinceEpoch = System.nanoTime() - t0NanoTime + t0NanosSinceEpoch;
        final long ts = ((nanosSinceEpoch) & 0xFFFFFFFFFFFFFFFEL) + (setLSB ? 1L : 0L);
        return ts;
    }

    public static final void sleepUntil(long untilTimeMillis, Logger log) throws StorageException {
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
