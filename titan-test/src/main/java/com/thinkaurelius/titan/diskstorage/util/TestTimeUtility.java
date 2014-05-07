package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.util.time.Timepoint;
import org.junit.Test;

import com.thinkaurelius.titan.util.time.Timestamps;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TestTimeUtility {


    @Test
    public void testTimeSequence() throws Exception {
        Random r = new Random();
        Timepoint[] times = new Timepoint[10];
        for (int i = 0; i < times.length; i++) {
            times[i] = Timestamps.NANO.getTime();
            if (i > 0) assertTrue(times[i] + " > " + times[i - 1], times[i].compareTo(times[i - 1])>0);
            Thread.sleep(r.nextInt(50) + 2);
        }
    }

}
