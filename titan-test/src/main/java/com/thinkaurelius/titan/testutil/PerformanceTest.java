package com.thinkaurelius.titan.testutil;

public class PerformanceTest {

    private long startTime = -1;
    private long endTime = -1;

    public PerformanceTest() {
        this(false);
    }

    public PerformanceTest(boolean start) {
        if (start) start();
    }

    public void start() {
        startTime = System.nanoTime();
    }

    public long end() {
        endTime = System.nanoTime();
        return getNanoTime();
    }

    public long getNanoTime() {
        assert startTime >= 0;
        if (endTime < 0) return System.nanoTime() - startTime;
        else return endTime - startTime;
    }

    public long getMicroTime() {
        return getNanoTime() / 1000;
    }

    public long getMiliTime() {
        return getNanoTime() / 1000000;
    }


}
