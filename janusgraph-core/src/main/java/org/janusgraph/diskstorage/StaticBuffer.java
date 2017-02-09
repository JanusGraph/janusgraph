// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage;

import org.janusgraph.diskstorage.util.StaticArrayBuffer;

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

    public boolean getBoolean(int position);

    public short getShort(int position);

    public int getInt(int position);

    public long getLong(int position);

    public char getChar(int position);

    public float getFloat(int position);

    public double getDouble(int position);

    public byte[] getBytes(int position, int length);

    public short[] getShorts(int position, int length);

    public int[] getInts(int position, int length);

    public long[] getLongs(int position, int length);

    public char[] getChars(int position, int length);

    public float[] getFloats(int position, int length);

    public double[] getDoubles(int position, int length);

    public StaticBuffer subrange(int position, int length);

    public StaticBuffer subrange(int position, int length, boolean invert);

    public ReadBuffer asReadBuffer();

    public<T> T as(Factory<T> factory);

    //Convenience method
    public ByteBuffer asByteBuffer();

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

    public static final Factory<ByteBuffer> BB_FACTORY = new Factory<ByteBuffer>() {
        @Override
        public ByteBuffer get(byte[] array, int offset, int limit) {
            return ByteBuffer.wrap(array, offset, limit - offset);
        }
    };

    public static final Factory<StaticBuffer> STATIC_FACTORY = new Factory<StaticBuffer>() {
        @Override
        public StaticBuffer get(byte[] array, int offset, int limit) {
            return new StaticArrayBuffer(array, offset, limit);
        }
    };

}
