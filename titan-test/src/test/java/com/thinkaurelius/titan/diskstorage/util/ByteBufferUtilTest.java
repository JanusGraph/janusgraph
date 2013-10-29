package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.primitives.Longs;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ByteBufferUtilTest {

    private static final Random random = new Random();


    @Test
    public void testCompareRandom() {
        int trials = 10000;
        for (int t = 0; t < trials; t++) {
            long val1 = Math.abs(random.nextLong());
            long val2 = Math.abs(random.nextLong());
            ByteBuffer b1 = ByteBufferUtil.getLongByteBuffer(val1);
            ByteBuffer b2 = ByteBufferUtil.getLongByteBuffer(val2);
            assertEquals(val1 + " : " + val2, Math.signum(Longs.compare(val1, val2)), Math.signum(ByteBufferUtil.compare(b1, b2)));
            assertEquals(Math.signum(Longs.compare(val2, val1)), Math.signum(ByteBufferUtil.compare(b2, b1)));
            assertEquals(Math.signum(Longs.compare(val1, val1)), Math.signum(ByteBufferUtil.compare(b1, b1)));
        }
    }


}
