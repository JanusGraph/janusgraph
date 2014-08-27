package com.thinkaurelius.titan.util;

import com.google.common.collect.Sets;
import com.thinkaurelius.titan.util.encoding.LongEncoding;
import org.junit.Test;

import java.util.Random;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LongEncodingTest {


    @Test
    public void testEncoding() {
        final int number = 1000000;
        final Random r = new Random();
        long start = System.currentTimeMillis();
        for (int i=0;i<number;i++) {
            long l = Math.abs(r.nextLong());
            if (l==Long.MIN_VALUE) continue;
            assertEquals(l, LongEncoding.decode(LongEncoding.encode(l)));
        }
        System.out.println("Time to de/encode "+number+" longs (in ms): " + (System.currentTimeMillis()-start));
    }

    @Test
    public void testCaseInsensitivity() {
        Set<String> codes = Sets.newHashSet();
        for (int i = 0; i < 500000; i++) {
            assertTrue(codes.add(LongEncoding.encode(i).toLowerCase()));
        }
    }



}
