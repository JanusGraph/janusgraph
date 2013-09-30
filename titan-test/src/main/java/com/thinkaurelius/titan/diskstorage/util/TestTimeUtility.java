package com.thinkaurelius.titan.diskstorage.util;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TestTimeUtility {


    @Test
    public void testTimeSequence() throws Exception {
        Random r = new Random();
        long[] times = new long[10];
        for (int i = 0; i < times.length; i++) {
            times[i] = TimeUtility.INSTANCE.getApproxNSSinceEpoch();
            if (i > 0) assertTrue(times[i] + " > " + times[i - 1], times[i] > times[i - 1]);
            Thread.sleep(r.nextInt(50) + 2);
        }
    }

}
