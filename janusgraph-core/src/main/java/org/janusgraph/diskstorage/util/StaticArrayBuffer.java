// Copyright 2019 JanusGraph Authors
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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;

import java.nio.ByteBuffer;

/**
 * Implementation of {@link StaticBuffer} against byte array.
 * <p>
 * The byte to primitive conversion code was copied from / is inspired by Kryo's Input class:
 * <a href="https://code.google.com/p/kryo/source/browse/trunk/src/com/esotericsoftware/kryo/io/Input.java">https://code.google.com/p/kryo/source/browse/trunk/src/com/esotericsoftware/kryo/io/Input.java</a>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StaticArrayBuffer implements StaticBuffer {

    private final byte[] array;
    private int offset;
    private int limit;

    public StaticArrayBuffer(byte[] array, int offset, int limit) {
        assert array != null;
        assert offset >= 0 && offset <= limit; // offset == limit implies a zero-length array
        assert limit <= array.length;

        this.array = array;
        this.offset = offset;
        this.limit = limit;
    }

    public StaticArrayBuffer(byte[] array) {
        this(array, 0, array.length);
    }

    public StaticArrayBuffer(byte[] array, int limit) {
        this(array, 0, limit);
    }

    public StaticArrayBuffer(StaticBuffer buffer) {
        this((StaticArrayBuffer)buffer);
    }

    public StaticArrayBuffer(StaticArrayBuffer buffer) {
        this(buffer.array, buffer.offset, buffer.limit);
    }

    public static StaticArrayBuffer of(byte[] array) {
        return new StaticArrayBuffer(array);
    }

    public static StaticArrayBuffer of(ByteBuffer b) {
        if (b.hasArray()) {
            return new StaticArrayBuffer(b.array(),b.arrayOffset()+b.position(),b.arrayOffset()+b.limit());
        } else {
            byte[] array = new byte[b.remaining()];
            b.mark();
            b.get(array);
            b.reset();
            return StaticArrayBuffer.of(array);
        }
    }

    //-------------------

    void reset(int newOffset, int newLimit) {
        assert newOffset >= 0 && newOffset <= newLimit;
        assert newLimit <= array.length;
        this.offset=newOffset;
        this.limit=newLimit;
    }

    private int require(int position, int size) {
        int base = position + offset;
        if (position<0)
            throw new ArrayIndexOutOfBoundsException("Position [" + position + "] must be nonnegative");
        if (base+size>limit)
            throw new ArrayIndexOutOfBoundsException("Required size [" + size + "] " +
                    "exceeds actual remaining size [" + (limit-base) + "]");
        assert base + size <= limit;
        return base;
    }

    @Override
    public int length() {
        return limit - offset;
    }

    /*
    ############## BULK READING ################
     */

    public void copyTo(byte[] dest, int destOffset) {
        System.arraycopy(array,offset,dest,destOffset,length());
    }

    @Override
    public StaticBuffer subrange(int position, int length) {
        return subrange(position, length, false);
    }

    @Override
    public StaticBuffer subrange(int position, int length, boolean invert) {
        if (position<0 || length<0 || (offset + position + length)>limit)
            throw new ArrayIndexOutOfBoundsException("Position ["+position+"] and or length ["+length+"] out of bounds");
        if (!invert) {
            return new StaticArrayBuffer(array, offset + position, offset + position + length);
        } else {
            byte[] inverted = new byte[length];
            System.arraycopy(array,offset+position,inverted,0,length);
            for (int i = 0; i < inverted.length; i++) {
                inverted[i]=(byte)~inverted[i];
            }
            return new StaticArrayBuffer(inverted);
        }
    }

    @Override
    public ReadBuffer asReadBuffer() {
        return new ReadArrayBuffer(this);
    }

    @Override
    public ByteBuffer asByteBuffer() {
        return as(StaticBuffer.BB_FACTORY);
    }

    @Override
    public <T> T as(Factory<T> factory) {
        return factory.get(array, offset, limit);
    }

    protected <T> T as(Factory<T> factory, int position, int length) {
        if (position<0 || length<0 || (offset + position + length)>limit)
            throw new ArrayIndexOutOfBoundsException("Position ["+position+"] and or length ["+length+"] out of bounds");
        return factory.get(array,offset+position,offset+position+length);
    }


    /*
    ############## READING PRIMITIVES ################
     */

    public static final int BYTE_LEN = 1;
    public static final int SHORT_LEN = 2;
    public static final int INT_LEN = 4;
    public static final int LONG_LEN = 8;
    public static final int CHAR_LEN = 2;
    public static final int FLOAT_LEN = 4;
    public static final int DOUBLE_LEN = 8;

    @Override
    public byte getByte(int position) {
        return array[require(position, BYTE_LEN)];
    }

    @Override
    public boolean getBoolean(int position) {
        return getByte(position) > 0;
    }

    @Override
    public short getShort(int position) {
        int base = require(position, SHORT_LEN);
        return (short) (((array[base++] & 0xFF) << 8) | (array[base] & 0xFF));
    }

    @Override
    public int getInt(int position) {
        int base = require(position, INT_LEN);
        return getInt(array, base);
    }

    public static int getInt(byte[] array, int offset) {
        return (array[offset++] & 0xFF) << 24
                | (array[offset++] & 0xFF) << 16
                | (array[offset++] & 0xFF) << 8
                | array[offset] & 0xFF;
    }

    public static void putInt(byte[] array, int offset, final int value) {
        array[offset++]= (byte)((value >> 24) & 0xFF);
        array[offset++]= (byte)((value >> 16) & 0xFF);
        array[offset++]= (byte)((value >> 8) & 0xFF);
        array[offset]= (byte)(value & 0xFF);
    }

    @Override
    public long getLong(int position) {
        int offset = require(position, LONG_LEN);
        return getLong(array,offset);
    }

    public static long getLong(byte[] array, int offset) {
        return (long) array[offset++] << 56 //
                | (long) (array[offset++] & 0xFF) << 48 //
                | (long) (array[offset++] & 0xFF) << 40 //
                | (long) (array[offset++] & 0xFF) << 32 //
                | (long) (array[offset++] & 0xFF) << 24 //
                | (array[offset++] & 0xFF) << 16 //
                | (array[offset++] & 0xFF) << 8 //
                | array[offset] & 0xFF;
    }

    public static void putLong(byte[] array, int offset, final long value) {
        array[offset++] = (byte) (value >> 56  );
        array[offset++] = (byte)((value >> 48  ) & 0xFF);
        array[offset++] = (byte)((value >> 40  ) & 0xFF);
        array[offset++] = (byte)((value >> 32  ) & 0xFF);
        array[offset++] = (byte)((value >> 24  ) & 0xFF);
        array[offset++] = (byte)((value >> 16  ) & 0xFF);
        array[offset++] = (byte)((value >> 8   ) & 0xFF);
        array[offset]   = (byte) (value  & 0xFF);
    }

    @Override
    public char getChar(int position) {
        return (char) getShort(position);
    }

    @Override
    public float getFloat(int position) {
        return Float.intBitsToFloat(getInt(position));
    }

    @Override
    public double getDouble(int position) {
        return Double.longBitsToDouble(getLong(position));
    }

    //-------- ARRAY METHODS

    @Override
    public byte[] getBytes(int position, int length) {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i]=getByte(position);
            position += BYTE_LEN;
        }
        return result;
    }

    public short[] getShorts(int position, int length) {
        short[] result = new short[length];
        for (int i = 0; i < length; i++) {
            result[i]=getShort(position);
            position += SHORT_LEN;
        }
        return result;
    }

    public int[] getInts(int position, int length) {
        int[] result = new int[length];
        for (int i = 0; i < length; i++) {
            result[i]=getInt(position);
            position += INT_LEN;
        }
        return result;
    }

    public long[] getLongs(int position, int length) {
        long[] result = new long[length];
        for (int i = 0; i < length; i++) {
            result[i]=getLong(position);
            position += LONG_LEN;
        }
        return result;
    }

    public char[] getChars(int position, int length) {
        char[] result = new char[length];
        for (int i = 0; i < length; i++) {
            result[i]=getChar(position);
            position += CHAR_LEN;
        }
        return result;
    }

    public float[] getFloats(int position, int length) {
        float[] result = new float[length];
        for (int i = 0; i < length; i++) {
            result[i]=getFloat(position);
            position += FLOAT_LEN;
        }
        return result;
    }

    public double[] getDoubles(int position, int length) {
        double[] result = new double[length];
        for (int i = 0; i < length; i++) {
            result[i]=getDouble(position);
            position += DOUBLE_LEN;
        }
        return result;
    }

    /*
    ############## EQUALS, HASHCODE & COMPARE ################
     */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof StaticBuffer)) return false;
        final StaticBuffer b = (StaticBuffer) o;
        return length() == b.length() && compareTo(b) == 0;
    }

    /**
     * Thread-safe hashcode method for StaticBuffer written according to
     * Effective Java 2e by Josh Bloch.
     *
     * @return hashcode for given StaticBuffer
     */
    @Override
    public int hashCode() {
        return hashCode(length());
    }

    protected int hashCode(int length) {
        Preconditions.checkArgument(length<=length());
        int result = 17;
        for (int i = offset; i < offset+length; i++) {
            result = 31 * result + (int)array[i];
        }
        return result;
    }


    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("0x");
        for (int i=offset;i<limit;i++) {
            s.append(String.format("%02X", array[i]));
        }
        return s.toString();
    }


    @Override
    public int compareTo(StaticBuffer other) {
        assert other instanceof StaticArrayBuffer;
        return compareTo((StaticArrayBuffer) other);
    }

    public int compareTo(StaticArrayBuffer other) {
        return compareTo(array, offset, limit, other.array, other.offset, other.limit);
    }

    public int compareTo(byte[] otherBuffer, int otherOffset, int otherLimit) {
        return compareTo(array, offset, limit, otherBuffer, otherOffset, otherLimit);
    }

    protected int compareTo(int length, StaticBuffer buffer, int bufferLen) {
        assert buffer instanceof StaticArrayBuffer;
        return compareTo(length, (StaticArrayBuffer)buffer, bufferLen);
    }

    protected int compareTo(int length, StaticArrayBuffer buffer, int bufferLen) {
        assert buffer!=null;
        Preconditions.checkArgument(length<=length() && bufferLen<=buffer.length());
        return compareTo(array, offset, offset+length, buffer.array, buffer.offset, buffer.offset+bufferLen);
    }

    private static int compareTo(byte[] buffer1, int offset1, int end1,
                                 byte[] buffer2, int offset2, int end2) {
        // Short circuit equal case
        int length1 = end1 - offset1;
        int length2 = end2 - offset2;
        if (buffer1 == buffer2 &&
                offset1 == offset2 &&
                length1 == length2) {
            return 0;
        }
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
            int a = (buffer1[i] & 0xff);
            int b = (buffer2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return length1 - length2;
    }
}
