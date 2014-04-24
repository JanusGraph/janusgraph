package com.thinkaurelius.titan.core.time;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
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
    private Timepoint start;
    private Timepoint stop;

    public Timer(final TimestampProvider times) {
        this.times = times;
    }

    public Timer start() {
        Preconditions.checkState(null == start, "Timer can only be started once");
        start = times.getTime();
        return this;
    }

    public long getStartTime(TimeUnit u) {
        Preconditions.checkState(null != start, "Timer never started");
        return start.getTime(u);
    }

    public Timepoint getStartTime() {
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
            return ZeroDuration.INSTANCE;
        }
        final TimeUnit u = times.getUnit();
        final long stopTimestamp = null == stop ? times.getTime(u) : stop.getTime(u);
        return new SimpleDuration(stopTimestamp - start.getTime(u), u);
    }

    public String toString() {
        TimeUnit u = times.getUnit();
        return String.format("%d %s", stop.getTime(u) - start.getTime(u), u);
    }

//    private static class LongsDuration implements Duration {
//
//        private final long begin;
//        private final long end;
//        private final TimeUnit unit;
//
//        private LongsDuration(long begin, long end, TimeUnit unit) {
//            this.begin = begin;
//            this.end = end;
//            this.unit = unit;
//        }
//
//        @Override
//        public int compareTo(Duration o) {
//            final long mine = getLength(unit);
//            final long theirs = o.getLength(unit);
//            if (mine < theirs) {
//                return -1;
//            } else if (theirs < mine) {
//                return 1;
//            }
//            return 0;
//        }
//
//        @Override
//        public long getLength(TimeUnit output) {
//            return output.convert(end - begin, unit);
//        }
//    }
}
