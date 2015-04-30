package com.thinkaurelius.titan.testutil;

import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProviders;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

/**
 * Created by bryn on 01/05/15.
 */
public class TestTimestamps {

    @Test
    public void testMicro() {
        testRoundTrip(TimestampProviders.MICRO);
        Assert.assertEquals(Instant.ofEpochSecond(1000), TimestampProviders.MICRO.getTime(1000000000));

    }

    @Test
    public void testMilli() {
        testRoundTrip(TimestampProviders.MILLI);
    }

    @Test
    public void testNano() {
        testRoundTrip(TimestampProviders.NANO);
    }

    private void testRoundTrip(TimestampProvider p) {
        Instant now = p.getTime();
        long time = p.getTime(now);
        Instant now2 = p.getTime(time);
        Assert.assertEquals(now, now2);
    }


}
