// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.util;

import com.google.common.primitives.Longs;
import org.janusgraph.diskstorage.StaticBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BufferUtilTest {

    private static final Random random = new Random();


    @Test
    public void testCompareRandom() {
        int trials = 100000;
        for (int t = 0; t < trials; t++) {
            long val1 = Math.abs(random.nextLong());
            long val2 = Math.abs(random.nextLong());
            StaticBuffer b1 = BufferUtil.getLongBuffer(val1);
            StaticBuffer b2 = BufferUtil.getLongBuffer(val2);

            //Compare
            assertEquals(Math.signum(Longs.compare(val1, val2)), Math.signum(b1.compareTo(b2)), 0.01, val1 + " : " + val2);
            assertEquals(Math.signum(Longs.compare(val2, val1)), Math.signum(b2.compareTo(b1)), 0.01);
            assertEquals(0, b1.compareTo(b1));

            ByteBuffer bb1 = of(val1);
            ByteBuffer bb2 = of(val2);
            assertEquals(Math.signum(Longs.compare(val1, val2)), Math.signum(ByteBufferUtil.compare(bb1,bb2)), 0.01, val1 + " : " + val2);
            assertEquals(Math.signum(Longs.compare(val2, val1)), Math.signum(ByteBufferUtil.compare(bb2, bb1)), 0.01);
            assertEquals(0, ByteBufferUtil.compare(bb1, bb1));


            //Mixed Equals
            if (0.5<Math.random()) val2=val1;
            ByteBuffer bb = of(val2);
            assertEquals(val1==val2,BufferUtil.equals(b1,bb));
        }
    }

    @Test
    public void testNextBigger() {
        int trials = 100000;
        for (int t = 0; t < trials; t++) {
            long val = random.nextLong()>>>1;
            assertTrue(val>=0);
            StaticBuffer b = BufferUtil.getLongBuffer(val);
            assertEquals(val,b.getLong(0));
            StaticBuffer bn = BufferUtil.nextBiggerBuffer(b);
            assertEquals(8,bn.length());
            assertEquals(val+1,bn.getLong(0));
        }

        try {
            StaticBuffer b = BufferUtil.getLongBuffer(-1);
            BufferUtil.nextBiggerBuffer(b);
            fail();
        } catch (IllegalArgumentException ignored) {}
        StaticBuffer b = BufferUtil.getLongBuffer(-1);
        StaticBuffer bn = BufferUtil.nextBiggerBufferAllowOverflow(b);
        assertEquals(8,bn.length());
        assertEquals(BufferUtil.zeroBuffer(8), bn);
    }

    public static ByteBuffer of(long val) {
        ByteBuffer bb = ByteBuffer.allocate(8).putLong(val);
        bb.flip();
        return bb;
    }

}
