package com.thinkaurelius.titan.util.encoding;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;

import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConversionHelper {

    public static final int getTTLSeconds(Duration duration) {
        Preconditions.checkArgument(duration!=null && !duration.isZeroLength(),"Must provide non-zero TTL");
        long ttlSeconds = Math.max(1,duration.getLength(TimeUnit.SECONDS));
        assert ttlSeconds>0;
        Preconditions.checkArgument(ttlSeconds<=Integer.MAX_VALUE, "tll value is too large [%s] - value overflow",duration);
        return (int)ttlSeconds;
    }

    public static final int getTTLSeconds(long time, TimeUnit unit) {
        return getTTLSeconds(new StandardDuration(time,unit));
    }

}
