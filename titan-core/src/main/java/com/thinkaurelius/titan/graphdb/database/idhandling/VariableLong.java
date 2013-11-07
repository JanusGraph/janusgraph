package com.thinkaurelius.titan.graphdb.database.idhandling;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VariableLong {

    public static int unsignedByte(byte b) {
        return b < 0 ? b + 256 : b;
    }

    //Move stop bit back to front => rewrite prefix variable encoding by custom writing first byte

    private static final byte BIT_MASK = 127;
    private static final byte STOP_MASK = -128;

    private static long readUnsigned(ReadBuffer in) {
        long value = 0;
        byte b;
        do {
            b = in.getByte();
            value = value << 7 | (b & BIT_MASK);
        } while (b >= 0);
        return value;
    }


    private static void writeUnsigned(WriteBuffer out, final long value) {
        writeUnsigned(out, unsignedBlockBitLength(value), value);
    }

    private static void writeUnsigned(WriteBuffer out, int offset, final long value) {
        assert offset % 7 == 0;
        while (offset > 0) {
            offset -= 7;
            byte b = (byte) ((value >>> offset) & BIT_MASK);
            if (offset == 0) {
                b = (byte) (b | STOP_MASK);
            }
            out.putByte(b);
        }
    }

    private static int unsignedBlockBitLength(final long value) {
        return unsignedNumBlocks(value)*7;
    }

    private static int unsignedNumBlocks(final long value) {
        return numVariableBlocks(unsignedBitLength(value));
    }

    private static int numVariableBlocks(final int numBits) {
        assert numBits > 0;
        return (numBits - 1) / 7 + 1;
    }

    public static int unsignedBitLength(final long value) {
        return (value == 0) ? 1 : Long.SIZE - Long.numberOfLeadingZeros(value);
    }

    /* ##################################
          Read and write positive longs
       ################################## */


    public static long readPositive(ReadBuffer in) {
        long value = readUnsigned(in);
        assert value >= 0;
        return value;
    }

    public static void writePositive(WriteBuffer out, final long value) {
        assert value >= 0;
        writeUnsigned(out, value);
    }

    public static StaticBuffer positiveByteBuffer(final long value) {
        WriteBuffer buffer = new WriteByteBuffer(positiveLength(value));
        writePositive(buffer, value);
        return buffer.getStaticBuffer();
    }


    public static StaticBuffer positiveByteBuffer(long[] value) {
        int len = 0;
        for (long aValue : value)
            len += positiveLength(aValue);

        WriteBuffer buffer = new WriteByteBuffer(len);

        for (long aValue : value)
            writePositive(buffer, aValue);

        return buffer.getStaticBuffer();
    }

    public static int positiveLength(long value) {
        assert value >= 0;
        return unsignedNumBlocks(value);
    }

    /* ##################################
      Read and write arbitrary longs
    ################################## */

    private static long convert2Unsigned(final long value) {
        assert value >= 0 || value > Long.MIN_VALUE;
        return Math.abs(value) << 1 | (value < 0 ? 1 : 0);
    }

    private static long convertFromUnsigned(final long value) {
        return ((value & 1) == 1) ? -(value >>> 1) : value >>> 1;
    }

    public static int length(long value) {
        return unsignedNumBlocks(convert2Unsigned(value));
    }

    public static void write(WriteBuffer out, final long value) {
        writeUnsigned(out, convert2Unsigned(value));
    }

    public static long read(ReadBuffer in) {
        return convertFromUnsigned(readUnsigned(in));
    }


    /* ##################################
      Read and write positive longs with a specified binary prefix of fixed length
    ################################## */

    public static void writePositiveWithPrefix(final WriteBuffer out, long value, long prefix, final int prefixBitLen) {
        assert value >= 0;
        assert prefixBitLen > 0 && prefixBitLen < 6 && (prefix < (1L << prefixBitLen));
        //Write first byte
        int deltaLen = 8 - prefixBitLen;
        byte first = (byte)(prefix<<deltaLen);
        int valueLen = unsignedBitLength(value);
        int mod = valueLen%7;
        if (mod<=(deltaLen-1)) {
            int offset = (valueLen-mod);
            first = (byte)(first | (value >>> offset));
            value = value & ((1l<<offset)-1);
            valueLen -= mod;
        } else {
            valueLen += (7-mod);
        }
        if (valueLen==0) {
            //Add stop mask
            first = (byte) ( first | (1<<(deltaLen-1)));
        }
        out.putByte(first);
        if (valueLen>0) {
            //Keep writing
            writeUnsigned(out,valueLen,value);
        }
    }

    public static int positiveWithPrefixLength(final long value, final int prefixBitLen) {
        assert value >= 0;
        assert prefixBitLen > 0 && prefixBitLen < 6;
        return numVariableBlocks(unsignedBitLength(value)+prefixBitLen);
    }

    public static long[] readPositiveWithPrefix(final ReadBuffer in, final int prefixBitLen) {
        assert prefixBitLen > 0 && prefixBitLen < 6;

        int first = unsignedByte(in.getByte());
        int deltaLen = 8 - prefixBitLen;
        long prefix = first>>deltaLen;
        long value =  first & ((1<<(deltaLen-1))-1);
        if ( ((first>>>(deltaLen-1)) & 1) == 0) { //No stop mask
            int deltaPos = in.getPosition();
            long remainder = readUnsigned(in);
            deltaPos = in.getPosition()-deltaPos;
            assert deltaPos > 0;
            value = (value<<(deltaPos*7)) + remainder;
        }
        return new long[]{value, prefix};
    }


    /* ##################################
      Write positive longs so that they can be read backwards
      Use positiveLength() for length
    ################################## */

    public static void writePositiveBackward(WriteBuffer out, long value) {
        assert value >= 0;
        writeUnsignedBackward(out,value);
    }

    public static long readPositiveBackward(ReadBuffer in) {
        return readUnsignedBackward(in);
    }

    /* ##################################
      Write arbitrary longs so that they can be read backwards
      Use length() for length
    ################################## */

    public static void writeBackward(WriteBuffer out, final long value) {
        writeUnsignedBackward(out, convert2Unsigned(value));
    }

    public static long readBackward(ReadBuffer in) {
        return convertFromUnsigned(readUnsignedBackward(in));
    }

    private static void writeUnsignedBackward(WriteBuffer out, long value) {
        boolean first = true;
        do {
            byte b = (byte) (value & BIT_MASK);
            value = value >>> 7;
            if (first) {
                b = (byte) (b | STOP_MASK);
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
            b = in.getByte(position);
            value = value << 7 | b & BIT_MASK;
            position--;
        } while (b >= 0);
        in.movePosition(position-in.getPosition());
        return value;
    }


}
