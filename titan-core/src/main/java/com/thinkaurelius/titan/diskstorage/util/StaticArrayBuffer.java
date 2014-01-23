package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

import java.nio.ByteBuffer;

/**
 * Implementation of {@link StaticBuffer} against byte array.
 * <p/>
 * The byte to primitive conversion code was copied from / is inspired by Kryo's Input class:
 * {@linktourl https://code.google.com/p/kryo/source/browse/trunk/src/com/esotericsoftware/kryo/io/Input.java}
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
        if (position<0 || base+size>limit) throw new ArrayIndexOutOfBoundsException("Position ["+position+"] and or size ["+size+"] out of bounds");
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

    void copyTo(byte[] dest, int destOffset) {
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

    @Override
    public byte getByte(int position) {
        return array[require(position, 1)];
    }

    @Override
    public short getShort(int position) {
        int base = require(position, 2);
        return (short) (((array[base++] & 0xFF) << 8) | (array[base++] & 0xFF));
    }

    @Override
    public int getInt(int position) {
        int base = require(position, 4);
        return (array[base++] & 0xFF) << 24 //
                | (array[base++] & 0xFF) << 16 //
                | (array[base++] & 0xFF) << 8 //
                | array[base++] & 0xFF;
    }

    @Override
    public long getLong(int position) {
        int base = require(position, 8);
        return (long) array[base++] << 56 //
                | (long) (array[base++] & 0xFF) << 48 //
                | (long) (array[base++] & 0xFF) << 40 //
                | (long) (array[base++] & 0xFF) << 32 //
                | (long) (array[base++] & 0xFF) << 24 //
                | (array[base++] & 0xFF) << 16 //
                | (array[base++] & 0xFF) << 8 //
                | array[base++] & 0xFF;
    }

    @Override
    public char getChar(int position) {
        int base = require(position, 2);
        return (char) (((array[base++] & 0xFF) << 8) | (array[base++] & 0xFF));
    }

    @Override
    public float getFloat(int position) {
        return Float.intBitsToFloat(getInt(position));
    }

    @Override
    public double getDouble(int position) {
        return Double.longBitsToDouble(getLong(position));
    }

    /*
    ############## EQUALS, HASHCODE & COMPARE ################
     */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof StaticBuffer)) return false;
        StaticBuffer b = (StaticBuffer)o;
        if (length()!=b.length()) return false;
        return compareTo(b)==0;
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
        return toString("-");
    }

    public final String toString(String separator) {
        StringBuilder s = new StringBuilder();
        for (int i=offset;i<limit;i++) {
            if (i>offset) s.append(separator);
            s.append(ByteBufferUtil.toFixedWidthString(array[i]));
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
