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

    public static final int unsignedByte(byte b) {
        if (b<0) return b + 256;
        else return b;
    }

    private static final byte stopMask = 1;

    private static long readUnsigned(ReadBuffer in) {
        long value = 0;
        int b;
        do {
            b = unsignedByte(in.getByte());
            value = value << 7;
            value = value | (b >>> 1);
        } while ((b & stopMask) == 0);
        return value;
    }


    private static void writeUnsigned(WriteBuffer out, final long value) {
        int offset = unsignedBlockBitLength(value);
        while (offset > 0) {
            offset -= 7;
            byte b = (byte) ((value >>> offset) << 1);
            if (offset == 0) {
                b = (byte) (b | stopMask);
            }
            out.putByte(b);
        }
    }

    private static int unsignedBlockBitLength(final long value) {
        return unsignedLength(value)*7;
    }

    private static int unsignedLength(final long value) {
        return numVariableBlocks(unsignedBitLength(value));
    }

    private static final int numVariableBlocks(final int numBits) {
        Preconditions.checkArgument(numBits>0);
        return (numBits-1)/7 + 1;
    }

    private static final int unsignedBitLength(final long value) {
        if (value==0) return 1;
        else return Long.SIZE-Long.numberOfLeadingZeros(value);
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

    public static void writePositiveWithPrefix(final WriteBuffer out, final long value, long prefix, final int prefixBitLen) {
        Preconditions.checkArgument(value>=0);
        Preconditions.checkArgument(prefixBitLen>0 && prefixBitLen<7 && prefix>0 && (prefix<(1l<<prefixBitLen)),"Invalid prefix [%s] for length [%s]",prefix,prefixBitLen);
        int valueBitLen = unsignedBitLength(value);
        int blocks = numVariableBlocks(valueBitLen+prefixBitLen);
        int offset = blocks*7-prefixBitLen;
        Preconditions.checkArgument(value<(1l<<offset));
        long newValue = (prefix<<offset) | value;
        writePositive(out,newValue);
    }

    public static int positiveWithPrefixLength(final long value, final int prefixBitLen) {
        Preconditions.checkArgument(value>=0);
        Preconditions.checkArgument(prefixBitLen>0 && prefixBitLen<7);
        return numVariableBlocks(unsignedBitLength(value)+prefixBitLen);
    }

    public static long[] readPositiveWithPrefix(final ReadBuffer in, final int prefixBitLen) {
        Preconditions.checkArgument(prefixBitLen>0 && prefixBitLen<7,"Invalid prefix bit length: %s",prefixBitLen);
        int posBefore = in.getPosition();
        long newValue = readPositive(in);
        int blocks = in.getPosition()-posBefore;
        Preconditions.checkArgument(blocks>0);
        int offset = blocks*7-prefixBitLen;
        return new long[]{newValue & ((1l<<offset)-1), (newValue>>>offset)};
    }

    //Experimental

    private static final byte SEVEN_BIT_MASK = Byte.MAX_VALUE;

    private static void writeUnsignedBackward(WriteBuffer out, long value) {
        boolean first = true;
        do {
            byte b = (byte) ((value & SEVEN_BIT_MASK) << 1);
            value = value >>> 7;
            if (first) {
                b = (byte) (b | stopMask);
                first = false;
            }
            out.putByte(b);
        } while (value>0);
    }

    private static long readUnsignedBackward(ReadBuffer in) {
        int position = in.getPosition();
        long value = 0;
        int b;
        do {
            b = unsignedByte(in.getByte(position));
            value = value << 7;
            value = value | (b >>> 1);
            position--;
        } while ((b & stopMask) == 0);
        in.movePosition(position-in.getPosition());
        return value;
    }

}
