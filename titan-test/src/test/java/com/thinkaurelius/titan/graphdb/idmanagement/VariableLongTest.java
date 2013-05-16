package com.thinkaurelius.titan.graphdb.idmanagement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import org.apache.commons.lang.time.StopWatch;
import org.junit.Test;

import java.nio.ByteBuffer;

import static junit.framework.Assert.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VariableLongTest {

    public void positiveWrite(long maxValue, long jump) {
        long allocate = maxValue / jump * 8;
        Preconditions.checkArgument(allocate < (1 << 28));
        WriteBuffer wb = new WriteByteBuffer((int) allocate);
        int num = 0;
        StopWatch w = new StopWatch();
        w.start();
        for (long i = 0; i < maxValue; i += jump) {
            VariableLong.writePositive(wb, i);
            num++;
        }
        //for (int i=0;i<b.remaining();i++) System.out.print(b.get(i)+"|");
        w.stop();
        ReadBuffer rb = wb.getStaticBuffer().asReadBuffer();
        System.out.println("Writing " + num + " longs in " + rb.length() + " bytes. in time: " + w.getTime());
        for (long i = 0; i < maxValue; i += jump) {
            long value = VariableLong.readPositive(rb);
            assertEquals(i, value);
        }
    }

    public void negativeWrite(long maxValue, long jump) {
        long allocate = maxValue / jump * 8 * 2;
        Preconditions.checkArgument(allocate < (1 << 28));
        WriteBuffer wb = new WriteByteBuffer((int) allocate);
        for (long i = -maxValue; i < maxValue; i += jump) {
            VariableLong.write(wb, i);
        }
        ReadBuffer rb = wb.getStaticBuffer().asReadBuffer();
        for (long i = -maxValue; i < maxValue; i += jump) {
            long value = VariableLong.read(rb);
            assertEquals(i, value);
        }
    }

    @Test
    public void testPositiveWriteBig() {
        positiveWrite(10000000000000L, 1000000L);
    }

    @Test
    public void testPositiveWriteSmall() {
        positiveWrite(1000000, 1);
    }

    @Test
    public void testNegativeWriteBig() {
        negativeWrite(1000000000000L, 1000000L);
    }

    @Test
    public void testNegativeWriteSmall() {
        negativeWrite(1000000, 1);
    }

    @Test
    public void testBoundary() {
        WriteBuffer wb = new WriteByteBuffer(512);
        VariableLong.write(wb, 0);
        VariableLong.write(wb, Long.MAX_VALUE);
        VariableLong.write(wb, -Long.MAX_VALUE);
        ReadBuffer rb = wb.getStaticBuffer().asReadBuffer();
        assertEquals(0, VariableLong.read(rb));
        assertEquals(Long.MAX_VALUE, VariableLong.read(rb));
        assertEquals(-Long.MAX_VALUE, VariableLong.read(rb));
    }

}
