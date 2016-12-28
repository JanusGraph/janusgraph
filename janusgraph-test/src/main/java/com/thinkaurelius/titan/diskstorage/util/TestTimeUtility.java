package com.thinkaurelius.titan.diskstorage.util;


import org.junit.Test;

import com.thinkaurelius.titan.diskstorage.util.time.TimestampProviders;

import java.time.Instant;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TestTimeUtility {


    @Test
    public void testTimeSequence() throws Exception {
        Random r = new Random();
        Instant[] times = new Instant[10];
        for (int i = 0; i < times.length; i++) {
            times[i] = TimestampProviders.NANO.getTime();
            if (i > 0) assertTrue(times[i] + " > " + times[i - 1], times[i].compareTo(times[i - 1])>0);
            Thread.sleep(r.nextInt(50) + 2);
        }
    }

}
