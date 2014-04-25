package com.thinkaurelius.titan.core.time;

import java.util.concurrent.TimeUnit;

public class Durations {

    public static Duration min(Duration x, Duration y) {
        return x.compareTo(y) <= 0 ? x : y;
    }

    /*
     * This method is based on the method of the same name in Stopwatch.java in
     * Google Guava 14.0.1, where it was defined with private visibility.
     */
    static String abbreviate(TimeUnit unit) {
        switch (unit) {
        case NANOSECONDS:
            return "ns";
        case MICROSECONDS:
            return "\u03bcs"; // μs
        case MILLISECONDS:
            return "ms";
        case SECONDS:
            return "s";
        case MINUTES:
            return "m";
        case HOURS:
            return "h";
        case DAYS:
            return "d";
        }
        throw new AssertionError();
    }
}
