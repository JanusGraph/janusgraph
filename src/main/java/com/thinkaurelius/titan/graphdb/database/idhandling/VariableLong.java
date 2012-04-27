package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class VariableLong {

    private static final byte mask = (1<<7)-1;
    private static final byte stopMask = -128;

    public static long readPositive(ByteBuffer in) {
        long value = 0;
        byte b = 0;
        do {
            b = in.get();
            value = value << 7;
            value = value | (b & mask);
        } while (b>=0);
        return value;
    }
    
    public static void writePositive(ByteBuffer out, final long value) {
        Preconditions.checkArgument(value>=0,"Positive value expected: " + value);
        int offset = 63;
        while (offset>0) {
            offset -= 7;
            long cut = value>>offset;
            if (cut>0 || offset==0) {
                byte b= (byte)(cut%128);
                if (offset==0) {
                    b = (byte) (b | stopMask);
                }
                out.put(b);
            }
        }
    }
    
    public static ByteBuffer positiveByteBuffer(final long value) {
        ByteBuffer buffer = ByteBuffer.allocate(positiveLength(value));
        writePositive(buffer,value);
        buffer.flip();
        return buffer;
    }


    public static ByteBuffer positiveByteBuffer(long[] value) {
        int len = 0;
        for (int i=0;i<value.length;i++) len+=positiveLength(value[i]);
        ByteBuffer buffer = ByteBuffer.allocate(len);
        for (int i=0;i<value.length;i++) writePositive(buffer,value[i]);
        buffer.flip();
        return buffer;
    }
    
    public static int positiveLength(long value) {
        assert value>=0;
        int length = 1;
        value = (value>>7);
        while (value>0) {
            value = (value>>7);
            length++;
        }
        return length;
    }

    public static int length(long value) {
        return positiveLength(Math.abs(value)*2 + (value<0?1:0));
    }
    
    public static void write(ByteBuffer out, final long value) {
        writePositive(out,Math.abs(value)*2 + (value<0?1:0));
    }

    public static long read(ByteBuffer in) {
        long value = readPositive(in);
        boolean neg = value%2==1;
        value = value/2;
        if (neg) value = -value;
        return value;
    }

    public static ByteBuffer byteBuffer(final long value) {
        ByteBuffer buffer = ByteBuffer.allocate(length(value));
        write(buffer,value);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer byteBuffer(long[] value) {
        int len = 0;
        for (int i=0;i<value.length;i++) len+=length(value[i]);
        ByteBuffer buffer = ByteBuffer.allocate(len);
        for (int i=0;i<value.length;i++) write(buffer,value[i]);
        buffer.flip();
        return buffer;
    }

    // =============== THIS IS A COPY&PASTE OF THE ABOVE =================
    // Using DataOutput instead of ByteBuffer

    public static void writePositive(DataOutput out, final long value) {
        Preconditions.checkArgument(value>=0,"Positive value expected: " + value);
        int offset = 63;
        while (offset>0) {
            offset -= 7;
            long cut = value>>offset;
            if (cut>0 || offset==0) {
                byte b= (byte)(cut%128);
                if (offset==0) {
                    b = (byte) (b | stopMask);
                }
                out.putByte(b);
            }
        }
    }

    public static void write(DataOutput out, final long value) {
        writePositive(out,Math.abs(value)*2 + (value<0?1:0));
    }
}
