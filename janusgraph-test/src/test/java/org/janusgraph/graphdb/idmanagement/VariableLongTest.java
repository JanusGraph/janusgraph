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

package org.janusgraph.graphdb.idmanagement;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.time.StopWatch;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VariableLongTest {


    private static final Logger log =
            LoggerFactory.getLogger(VariableLongTest.class);

    private void readWriteTest(final ReadWriteLong impl, long maxValue, long jump, boolean negative, boolean backward) {
        Preconditions.checkArgument(maxValue%jump==0);
        long allocate = maxValue / jump * 8 * (negative?2:1);
        Preconditions.checkArgument(allocate < (1 << 28));
        WriteBuffer wb = new WriteByteBuffer((int) allocate);
        int num = 0;
        StopWatch w = new StopWatch();
        w.start();
        for (long i = (negative?-maxValue:0); i <= maxValue; i += jump) {
            impl.write(wb, i);
            num++;
        }
        //for (int i=0;i<b.remaining();i++) System.out.print(b.get(i)+"|");
        w.stop();
        ReadBuffer rb = wb.getStaticBuffer().asReadBuffer();
        log.info("Writing " + num + " longs in " + rb.length() + " bytes. in time: " + w.getTime());

        final ReadVerify read = (rb1, expected) -> {
            int beforePos = rb1.getPosition();
            long value = impl.read(rb1);
            assertEquals(expected, value);
            int length = Math.abs(rb1.getPosition()-beforePos);
            assertEquals(length,impl.length(expected), "On: " + expected);
        };

        if (backward) {
            rb.movePositionTo(rb.length());
            for (long i = maxValue; i != (negative?-maxValue:0); i -= jump) {
                read.next(rb,i);
            }
        } else {
            for (long i = (negative?-maxValue:0); i <= maxValue; i += jump) {
                read.next(rb, i);
            }
        }

        //Test boundaries
        wb = new WriteByteBuffer(512);
        impl.write(wb,0);
        impl.write(wb,Long.MAX_VALUE);
        if (negative) impl.write(wb,-Long.MAX_VALUE);
        rb = wb.getStaticBuffer().asReadBuffer();
        if (backward) {
            rb.movePositionTo(rb.length());
            if (negative) assertEquals(-Long.MAX_VALUE, impl.read(rb));
            assertEquals(Long.MAX_VALUE, impl.read(rb));
            assertEquals(0, impl.read(rb));
        } else {
            assertEquals(0, impl.read(rb));
            assertEquals(Long.MAX_VALUE, impl.read(rb));
            if (negative) assertEquals(-Long.MAX_VALUE, impl.read(rb));
        }
    }

    private interface ReadVerify {
        void next(ReadBuffer rb, long expected);
    }


    public void positiveReadWriteTest(ReadWriteLong impl, long maxValue, long jump) {
        readWriteTest(impl,maxValue,jump,false,false);
    }

    public void negativeReadWriteTest(ReadWriteLong impl, long maxValue, long jump) {
        readWriteTest(impl,maxValue,jump,true,false);
    }

    @Test
    public void testPositiveWriteBig() {
        positiveReadWriteTest(new PositiveReadWrite(), 10000000000000L, 1000000L);
    }

    @Test
    public void testPositiveWriteSmall() {
        positiveReadWriteTest(new PositiveReadWrite(),1000000, 1);
    }

    @Test
    public void testNegativeWriteBig() {
        negativeReadWriteTest(new NegativeReadWrite(), 1000000000000L, 1000000L);
    }

    @Test
    public void testNegativeWriteSmall() {
        negativeReadWriteTest(new NegativeReadWrite(), 1000000, 1);
    }

    @Test
    public void testPosBackwardWriteBig() {
        readWriteTest(new PosBackwardReadWrite(), 10000000000000L, 1000000L, false, true);
    }

    @Test
    public void testPosBackwardWriteSmall() {
        readWriteTest(new PosBackwardReadWrite(), 1000000, 1, false, true);
    }

    @Test
    public void testBackwardWriteBig() {
        readWriteTest(new BackwardReadWrite(), 10000000000000L, 10000000L, true, true);
    }

    @Test
    public void testBackwardWriteSmall() {
        readWriteTest(new BackwardReadWrite(), 1000000, 1, true, true);
    }

    @Test
    public void testPrefix1WriteBig() {
        positiveReadWriteTest(new PrefixReadWrite(3,4), 1000000000000L, 1000000L);
    }

    @Test
    public void testPrefix2WriteTiny() {
        positiveReadWriteTest(new PrefixReadWrite(2,1),130, 1);
    }

    @Test
    public void testPrefix2WriteSmall() {
        positiveReadWriteTest(new PrefixReadWrite(2,1),100000, 1);
    }

    @Test
    public void testPrefix3WriteSmall() {
        positiveReadWriteTest(new PrefixReadWrite(2,0),100000, 1);
    }


    public interface ReadWriteLong {

        void write(WriteBuffer out, long value);

        int length(long value);

        long read(ReadBuffer in);

    }

    public static class PositiveReadWrite implements ReadWriteLong {

        @Override
        public void write(WriteBuffer out, long value) {
            VariableLong.writePositive(out,value);
        }

        @Override
        public int length(long value) {
            return VariableLong.positiveLength(value);
        }

        @Override
        public long read(ReadBuffer in) {
            return VariableLong.readPositive(in);
        }
    }

    public static class NegativeReadWrite implements ReadWriteLong {

        @Override
        public void write(WriteBuffer out, long value) {
            VariableLong.write(out,value);
        }

        @Override
        public int length(long value) {
            return VariableLong.length(value);
        }

        @Override
        public long read(ReadBuffer in) {
            return VariableLong.read(in);
        }
    }

    public static class PosBackwardReadWrite implements ReadWriteLong {

        @Override
        public void write(WriteBuffer out, long value) {
            VariableLong.writePositiveBackward(out,value);
        }

        @Override
        public int length(long value) {
            return VariableLong.positiveBackwardLength(value);
        }

        @Override
        public long read(ReadBuffer in) {
            return VariableLong.readPositiveBackward(in);
        }
    }

    public static class BackwardReadWrite implements ReadWriteLong {

        @Override
        public void write(WriteBuffer out, long value) {
            VariableLong.writeBackward(out,value);
        }

        @Override
        public int length(long value) {
            return VariableLong.backwardLength(value);
        }

        @Override
        public long read(ReadBuffer in) {
            return VariableLong.readBackward(in);
        }
    }


    public static class PrefixReadWrite implements ReadWriteLong {

        private final int prefixLen;
        private final int prefix;

        public PrefixReadWrite(int prefixLen, int prefix) {
            Preconditions.checkArgument(prefixLen>0);
            Preconditions.checkArgument(prefix>=0 && prefix<(1<<prefixLen));
            this.prefixLen = prefixLen;
            this.prefix = prefix;
        }


        @Override
        public void write(WriteBuffer out, long value) {
            VariableLong.writePositiveWithPrefix(out,value,prefix,prefixLen);
        }

        @Override
        public int length(long value) {
            return VariableLong.positiveWithPrefixLength(value,prefixLen);
        }

        @Override
        public long read(ReadBuffer in) {
            long[] result = VariableLong.readPositiveWithPrefix(in,prefixLen);
            assertEquals(prefix,result[1]);
            return result[0];
        }
    }



    @Test
    public void byteOrderPreservingPositiveBackward() {
        long[] scalingFactors = { Long.MAX_VALUE, 1000, 1000000000L};
        for (int t=0;t<10000000;t++) {
            StaticBuffer[] b = new StaticBuffer[2];
            long[] l = new long[2];
            for (int i=0;i<2;i++) {
                l[i] = randomPosLong(scalingFactors[random.nextInt(scalingFactors.length)]);
                WriteBuffer out = new WriteByteBuffer(11);
                VariableLong.writePositiveBackward(out,l[i]);
                b[i]=out.getStaticBuffer();
                ReadBuffer res = b[i].asReadBuffer();
                res.movePositionTo(res.length());
                assertEquals(l[i], VariableLong.readPositiveBackward(res));
            }
//            System.out.println(l[0] + " vs " + l[1]);
            assertEquals(Math.signum(Long.compare(l[0],l[1])),Math.signum(b[0].compareTo(b[1])), 0.01);
        }

    }

    private static final Random random = new Random();

    public static long randomPosLong(long scaling) {
        long l = Math.round(random.nextGaussian()/3*scaling);
        if (l<0) l=Math.abs(l+1);
        assert l>=0;
        return l;
    }

}
