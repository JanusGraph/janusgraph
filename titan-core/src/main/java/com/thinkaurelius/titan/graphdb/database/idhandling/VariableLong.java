package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VariableLong {

    private static final byte mask = 127;
    private static final byte stopMask = -128;

    private static long readUnsigned(ReadBuffer in) {
        long value = 0;
        byte b;
        do {
            b = in.getByte();
            value = value << 7;
            value = value | (b & mask);
        } while (b >= 0);
        return value;
    }


    private static void writeUnsigned(WriteBuffer out, final long value) {
        int offset = unsignedBitLength(value);
        while (offset > 0) {
            offset -= 7;
            byte b = (byte) ((value >>> offset) & mask);
            if (offset == 0) {
                b = (byte) (b | stopMask);
            }
            out.putByte(b);
        }
    }

    private static int unsignedBitLength(final long value) {
        int length = 7;
        while ((value >>> length) > 0) {
            length += 7;
        }
        return length;
    }


    private static int unsignedLength(final long value) {
        assert unsignedBitLength(value) % 7 == 0 && unsignedBitLength(value) > 0;
        return unsignedBitLength(value) / 7;
    }


    public static long readPositive(ReadBuffer in) {
        long value = readUnsigned(in);
        assert value >= 0;
        return value;
    }

    public static void writePositive(WriteBuffer out, final long value) {
        Preconditions.checkArgument(value >= 0, "Positive value expected: " + value);
        writeUnsigned(out, value);
    }

    public static StaticBuffer positiveByteBuffer(final long value) {
        WriteBuffer buffer = new WriteByteBuffer(positiveLength(value));
        writePositive(buffer, value);
        return buffer.getStaticBuffer();
    }


    public static StaticBuffer positiveByteBuffer(long[] value) {
        int len = 0;
        for (int i = 0; i < value.length; i++) len += positiveLength(value[i]);
        WriteBuffer buffer = new WriteByteBuffer(len);
        for (int i = 0; i < value.length; i++) writePositive(buffer, value[i]);
        return buffer.getStaticBuffer();
    }

    public static int positiveLength(long value) {
        assert value >= 0;
        return unsignedLength(value);
    }

    private static long convert2Unsigned(long value) {
        assert value >= 0 || value > Long.MIN_VALUE;
        return Math.abs(value) << 1 | (value < 0 ? 1 : 0);
    }

    public static int length(long value) {
        return unsignedLength(convert2Unsigned(value));
    }

    public static void write(WriteBuffer out, final long value) {
        writeUnsigned(out, convert2Unsigned(value));
    }

    public static long read(ReadBuffer in) {
        long value = readUnsigned(in);
        if ((value & 1) == 1) return -(value >>> 1);
        else return value >>> 1;
    }

}
