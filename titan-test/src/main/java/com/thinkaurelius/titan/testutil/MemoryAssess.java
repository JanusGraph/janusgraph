package com.thinkaurelius.titan.testutil;

public class MemoryAssess {

    private long memStart = 0;

    public MemoryAssess() {
    }

    public MemoryAssess start() {
        memStart = getMemoryUse();
        return this;
    }

    public long end() {
        return getMemoryUse() - memStart;
    }

    private static long fSLEEP_INTERVAL = 100;

    public static long getMemoryUse() {
        putOutTheGarbage();
        putOutTheGarbage();
        long totalMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        return (totalMemory - freeMemory);
    }

    private static void putOutTheGarbage() {
        collectGarbage();
        collectGarbage();
    }

    private static void collectGarbage() {
        try {
            System.gc();
            Thread.sleep(fSLEEP_INTERVAL);
            System.runFinalization();
            Thread.sleep(fSLEEP_INTERVAL);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
}
