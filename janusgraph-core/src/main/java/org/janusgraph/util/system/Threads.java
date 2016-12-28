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

    public static final void terminate(Thread[] threads) {
        for (int i = 0; i < threads.length; i++) {
            if (threads[i] != null && threads[i].isAlive()) threads[i].interrupt();
        }
    }

    public static final int DEFAULT_SLEEP_INTERVAL_MS = 100;

    public static final boolean waitForCompletion(Thread[] threads) {
        return waitForCompletion(threads,Integer.MAX_VALUE);
    }

    public static final boolean waitForCompletion(Thread[] threads, int maxWaitMillis) {
        return waitForCompletion(threads,maxWaitMillis,DEFAULT_SLEEP_INTERVAL_MS);
    }

    public static final boolean waitForCompletion(Thread[] threads, int maxWaitMillis, int sleepPeriodMillis) {
        long endTime = System.currentTimeMillis()+maxWaitMillis;
        while (oneAlife(threads)) {
            long currentTime = System.currentTimeMillis();
            if (currentTime>=endTime) return false;
            try {
                Thread.sleep(Math.min(sleepPeriodMillis,endTime-currentTime));
            } catch (InterruptedException e) {
                System.err.println("Interrupted while waiting for completion of threads!");
            }
        }
        return true;
    }

}
