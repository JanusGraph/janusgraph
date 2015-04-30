package com.thinkaurelius.titan.diskstorage.util.time;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * A utility to measure time durations.
 * </p>
 * Differs from Guava Stopwatch in the following ways:
 *
 * <ul>
 * <li>encapsulates longs behind Instant/Duration</li>
 * <li>replacing the underlying Ticker with a TimestampProvider</li>
 * <li>can only be started and stopped once</li>
 * </ul>
 */
public class Timer {

    private final TimestampProvider times;
    private Instant start;
    private Instant stop;

    public Timer(final TimestampProvider times) {
        this.times = times;
    }

    public Timer start() {
        Preconditions.checkState(null == start, "Timer can only be started once");
        start = times.getTime();
        return this;
    }

    public Instant getStartTime() {
        Preconditions.checkState(null != start, "Timer never started");
        return start;
    }

    public Timer stop() {
        Preconditions.checkState(null != start, "Timer stopped before it was started");
        stop = times.getTime();
        return this;
    }

    public Duration elapsed() {
        if (null == start) {
            return Duration.ZERO;
        }
        final Instant stopTime = (null==stop? times.getTime() : stop);
        return Duration.between(start, stopTime);
    }

    public String toString() {
        TemporalUnit u = times.getUnit();
        if (start==null) return "Initialized";
        if (stop==null) return String.format("Started at %d %s",times.getTime(start),u);
        return String.format("%d %s", times.getTime(stop) - times.getTime(start), u);
    }
}
