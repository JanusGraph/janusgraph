package com.thinkaurelius.titan.util.encoding;

import com.google.common.base.Preconditions;


import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConversionHelper {

    public static final int getTTLSeconds(Duration duration) {
        Preconditions.checkArgument(duration!=null && !duration.isZero(),"Must provide non-zero TTL");
        long ttlSeconds = Math.max(1,duration.getSeconds());
        assert ttlSeconds>0;
        Preconditions.checkArgument(ttlSeconds<=Integer.MAX_VALUE, "tll value is too large [%s] - value overflow",duration);
        return (int)ttlSeconds;
    }

    public static final int getTTLSeconds(long time, TemporalUnit unit) {
        return getTTLSeconds(Duration.of(time,unit));
    }

}
