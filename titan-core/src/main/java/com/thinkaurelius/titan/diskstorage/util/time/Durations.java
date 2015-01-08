package com.thinkaurelius.titan.diskstorage.util.time;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods for dealing with {@link com.thinkaurelius.titan.core.attribute.Duration}
 */
public class Durations {

    public static Duration min(Duration x, Duration y) {
        return x.compareTo(y) <= 0 ? x : y;
    }

    /*
     * This method is based on the method of the same name in Stopwatch.java in
     * Google Guava 14.0.1, where it was defined with private visibility.
     */
    public static String abbreviate(TimeUnit unit) {
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
        default:
            throw new AssertionError("Unexpected time unit: " + unit);
        }

    }

    private static final Map<String,TimeUnit> unitNames = new HashMap<String,TimeUnit>() {{
        for (TimeUnit unit : TimeUnit.values()) {
            put(abbreviate(unit),unit); //abbreviated name
            String name = unit.toString().toLowerCase();
            put(name,unit); //abbreviated name in singular
            assert name.endsWith("s");
            put(name.substring(0,name.length()-1),unit);
        }
        put("us",TimeUnit.MICROSECONDS);
    }};

    public static TimeUnit parse(String unitName) {
        TimeUnit unit = unitNames.get(unitName.toLowerCase());
        Preconditions.checkArgument(unit!=null,"Unknown unit time: %s",unitName);
        return unit;
    }

    public static int compare(long length1, TimeUnit unit1, long length2, TimeUnit unit2) {
        /*
         * Don't do this:
         *
         * return (int)(o.getLength(unit) - getLength(unit));
         *
         * 2^31 ns = 2.14 seconds and 2^31 us = 36 minutes. The narrowing cast
         * from long to integer is practically guaranteed to cause failures at
         * either nanosecond resolution (where almost everything will fail) or
         * microsecond resolution (where the failures would be more insidious;
         * perhaps lock expiration malfunctioning).
         *
         * The following implementation is ugly, but unlike subtraction-based
         * implementations, it is affected by neither arithmetic overflow
         * (because it does no arithmetic) nor loss of precision from
         * long-to-integer casts (because it does not cast).
         */
        final long length2Adj = unit1.convert(length2,unit2);
        if (length1 < length2Adj) {
            return -1;
        } else if (length2Adj < length1) {
            return 1;
        }
        return 0;
    }

}
