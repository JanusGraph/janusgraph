package com.thinkaurelius.titan.util.time;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods for dealing with {@link Duration}
 */
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

}
