package com.thinkaurelius.titan.util;

import com.thinkaurelius.titan.util.encoding.LongEncoding;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LongEncodingTest {


    @Test
    public void testEncoding() {
        int number = 1000000;
        Random r = new Random();
        long start = System.currentTimeMillis();
        for (int i=0;i<number;i++) {
            long l = Math.abs(r.nextLong());
            assertEquals(l, LongEncoding.decode(LongEncoding.encode(l)));
        }
        System.out.println("Time to de/encode "+number+" longs (in ms): " + (System.currentTimeMillis()-start));
    }

}
