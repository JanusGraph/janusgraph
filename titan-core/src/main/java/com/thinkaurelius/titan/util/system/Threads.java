package com.thinkaurelius.titan.util.system;

/**
 * Utility class for dealing with {@link Thread}
 */
public class Threads {

    public static final boolean oneAlife(Thread[] threads) {
        for (int i = 0; i < threads.length; i++) {
            if (threads[i] != null && threads[i].isAlive()) return true;
        }
        return false;
    }


    public static final void waitForCompletion(Thread[] threads, int sleepPeriodMillis) {
        while (oneAlife(threads)) {
            try {
                Thread.sleep(sleepPeriodMillis);
            } catch (InterruptedException e) {
                System.err.println("Interrupted while waiting for completion of threads!");
            }
        }
    }

}
