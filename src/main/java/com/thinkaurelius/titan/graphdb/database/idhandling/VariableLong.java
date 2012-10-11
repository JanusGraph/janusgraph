package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class VariableLong {

    private static final byte mask = 127;
    private static final byte stopMask = -128;

    private static long readUnsigned(ByteBuffer in) {
        int offset = 0;
        long value = 0;
        byte b = 0;
        do {
            b = in.get();
            long add = (b & mask);
            add = add<<offset;
            offset+=7;
            value = value | add;
        } while (b>=0);
        return value;
    }


    private static void writeUnsigned(ByteBuffer out, long value) {
        boolean first = true;
        while (value!=0 || first) {
            first = false;
            byte b = (byte)(value & mask);
            value = value >>> 7;
            if (value==0) b = (byte)(b | stopMask);
            out.put(b);
        }
    }

    private static int unsignedLength(long value) {
        int length = 1;
        value = (value>>>7);
        while (value!=0) {
            value = (value>>>7);
            length++;
        }
        return length;
    }

    public static long readPositive(ByteBuffer in) {
        long value = readUnsigned(in);
        assert value>=0;
        return value;
    }

    public static void writePositive(ByteBuffer out, final long value) {
        Preconditions.checkArgument(value>=0,"Positive value expected: " + value);
        writeUnsigned(out,value);
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
        return unsignedLength(value);
    }
    
    private static long convert2Unsigned(long value) {
        assert value>=0 || value>Long.MIN_VALUE;
        return Math.abs(value)<<1 | (value<0?1:0);
    }

    public static int length(long value) {
        return unsignedLength(convert2Unsigned(value));
    }
    
    public static void write(ByteBuffer out, final long value) {
        writeUnsigned(out,convert2Unsigned(value));
    }

    public static long read(ByteBuffer in) {
        long value = readUnsigned(in);
        if ((value & 1) == 1) return -(value>>>1);
        else return value>>>1;
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

    public static void writeUnsigned(DataOutput out, long value) {
        boolean first = true;
        while (value!=0 || first) {
            first = false;
            byte b = (byte)(value & mask);
            value = value >>> 7;
            if (value==0) b = (byte)(b | stopMask);
            out.putByte(b);
        }
    }

    public static void writePositive(DataOutput out, long value) {
        Preconditions.checkArgument(value>=0,"Positive value expected: " + value);
        writeUnsigned(out,value);
    }

    public static void write(DataOutput out, final long value) {
        writeUnsigned(out,convert2Unsigned(value));
    }
}
