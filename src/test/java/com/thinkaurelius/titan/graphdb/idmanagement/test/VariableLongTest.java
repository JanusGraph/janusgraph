package com.thinkaurelius.titan.graphdb.idmanagement.test;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import junit.framework.TestCase;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class VariableLongTest extends TestCase {
    
    public void positiveWrite(long maxValue, long jump) {
        long allocate = maxValue/jump*8;
        Preconditions.checkArgument(allocate < (1<<28));
        ByteBuffer b = ByteBuffer.allocate((int)allocate);
        for (long i=0;i<maxValue;i+=jump) {
            VariableLong.writePositive(b,i);
        }
        b.rewind();
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
        b.rewind();
        for (long i=-maxValue;i<maxValue;i+=jump) {
            long value = VariableLong.read(b);
            assertEquals(i,value);
        }
    }
    
    public void testPositiveWriteBig() {
        positiveWrite(10000000000000L,10000000L);
    }

    public void testPositiveWriteSmall() {
        positiveWrite(1000000,1);
    }

    public void testNegativeWriteBig() {
        negativeWrite(1000000000000L,10000000L);
    }

    public void testNegativeWriteSmall() {
        negativeWrite(1000000,1);
    }
    
    
}
