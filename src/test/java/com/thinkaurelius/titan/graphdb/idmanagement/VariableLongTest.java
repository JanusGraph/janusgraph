package com.thinkaurelius.titan.graphdb.idmanagement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import junit.framework.TestCase;
import org.apache.commons.lang.time.StopWatch;
import org.junit.Test;

import java.nio.ByteBuffer;

import static junit.framework.Assert.assertEquals;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class VariableLongTest {
    
    public void positiveWrite(long maxValue, long jump) {
        long allocate = maxValue/jump*8;
        Preconditions.checkArgument(allocate < (1<<28));
        ByteBuffer b = ByteBuffer.allocate((int)allocate);
        int num = 0;
        StopWatch w = new StopWatch();
        w.start();
        for (long i=0;i<maxValue;i+=jump) {
            VariableLong.writePositive(b,i);
            num++;
        }
        b.flip();
        //for (int i=0;i<b.remaining();i++) System.out.print(b.get(i)+"|");
        w.stop();
        System.out.println("Writing " + num + " longs in " + b.limit() + " bytes. in time: " + w.getTime());
        for (long i=0;i<maxValue;i+=jump) {
            long value = VariableLong.readPositive(b);
            assertEquals(i,value);
        }
    }

    public void negativeWrite(long maxValue, long jump) {
        long allocate = maxValue/jump*8*2;
        Preconditions.checkArgument(allocate < (1<<28));
        ByteBuffer b = ByteBuffer.allocate((int)allocate);
        for (long i=-maxValue;i<maxValue;i+=jump) {
            VariableLong.write(b,i);
        }
        b.flip();
        for (long i=-maxValue;i<maxValue;i+=jump) {
            long value = VariableLong.read(b);
            assertEquals(i,value);
        }
    }

    @Test
    public void testPositiveWriteBig() {
        positiveWrite(10000000000000L,1000000L);
    }

    @Test
    public void testPositiveWriteSmall() {
        positiveWrite(1000000,1);
    }

    @Test
    public void testNegativeWriteBig() {
        negativeWrite(1000000000000L,1000000L);
    }

    @Test
    public void testNegativeWriteSmall() {
        negativeWrite(1000000,1);
    }

    @Test
    public void testBoundary() {
        ByteBuffer b = ByteBuffer.allocate(512);
        VariableLong.write(b,0);
        VariableLong.write(b,Long.MAX_VALUE);
        VariableLong.write(b,-Long.MAX_VALUE);
        b.flip();
        assertEquals(0,VariableLong.read(b));
        assertEquals(Long.MAX_VALUE,VariableLong.read(b));
        assertEquals(-Long.MAX_VALUE,VariableLong.read(b));
    }

}
