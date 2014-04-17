package com.thinkaurelius.titan.diskstorage.time;

import com.google.common.base.Preconditions;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TestTimestamps {

    @Test
    public void testTimeSequence() throws Exception {
        Random r = new Random();
        long[] times = new long[10];
        for (int i = 0; i < times.length; i++) {
            times[i] = Timestamps.NANO.getTime();
            if (i > 0) assertTrue(times[i] + " > " + times[i - 1], times[i] > times[i - 1]);
            Thread.sleep(r.nextInt(50) + 2);
        }
    }

    public static final void setTestTimestampProvider(TimestampProvider tp) {
        Preconditions.checkNotNull(tp);
        Timestamps.SYSTEM_TIMESTAMP = tp;
    }

}
