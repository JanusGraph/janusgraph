package com.thinkaurelius.titan.diskstorage;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A Buffer that only allows static access. This Buffer is immutable if
 * any returned byte array or ByteBuffer is not mutated.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface StaticBuffer extends Comparable<StaticBuffer> {

    public int length();

    public byte getByte(int position);

    public short getShort(int position);

    public int getInt(int position);

    public long getLong(int position);

    public char getChar(int position);

    public float getFloat(int position);

    public double getDouble(int position);

    public StaticBuffer subrange(int position, int length);

    public ReadBuffer asReadBuffer();

    public ByteBuffer asByteBuffer();

    public<T> T as(Factory<T> factory);

    public interface Factory<T> {

        public T get(byte[] array, int offset, int limit);

    }

    public static final Factory<byte[]> ARRAY_FACTORY = new Factory<byte[]>() {
        @Override
        public byte[] get(byte[] array, int offset, int limit) {
            if (offset==0 && limit==array.length) return array;
            else return Arrays.copyOfRange(array,offset,limit);
        }

    };

}
