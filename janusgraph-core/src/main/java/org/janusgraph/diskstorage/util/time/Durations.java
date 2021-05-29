// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.util.time;

import com.google.common.base.Preconditions;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
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
    public static String abbreviate(ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return "ns";
            case MICROS:
                return "\u03bcs"; // μs
            case MILLIS:
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

    private static final Map<String,TemporalUnit> unitNames = new HashMap<String,TemporalUnit>() {{
        for (ChronoUnit unit : Arrays.asList(ChronoUnit.NANOS, ChronoUnit.MICROS, ChronoUnit.MILLIS, ChronoUnit.SECONDS, ChronoUnit.MINUTES, ChronoUnit.HOURS, ChronoUnit.DAYS)) {
            put(abbreviate(unit),unit); //abbreviated name
            String name = unit.toString().toLowerCase();
            put(name,unit); //abbreviated name in singular
            assert name.endsWith("s");
            put(name.substring(0,name.length()-1),unit);
        }
        put("us",ChronoUnit.MICROS);
    }};

    public static TemporalUnit parse(String unitName) {
        TemporalUnit unit = unitNames.get(unitName.toLowerCase());
        Preconditions.checkNotNull(unit,"Unknown unit time: %s",unitName);
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
