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

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.ReadBuffer;

/**
 * Implementation of {@link ReadBuffer} against a byte array.
 *
 * Note, that the position does not impact the state of the object. Meaning, equals, hashcode,
 * and compare ignore the position.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ReadArrayBuffer extends StaticArrayBuffer implements ReadBuffer {

    public ReadArrayBuffer(byte[] array) {
        super(array);
    }

    ReadArrayBuffer(StaticArrayBuffer buffer) {
        super(buffer);
    }

    protected ReadArrayBuffer(byte[] array, int limit) {
        super(array, 0,limit);
    }

    @Override
    void reset(int newOffset, int newLimit) {
        position=0;
        super.reset(newOffset,newLimit);
    }

    /*
    ############ IDENTICAL CODE #############
     */


    private transient int position=0;

    private int updatePos(int update) {
        int pos = position;
        position+=update;
        return pos;
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public boolean hasRemaining() {
        return position<length();
    }

    @Override
    public void movePositionTo(int newPosition) {
        assert newPosition >= 0 && newPosition <= length();
        position = newPosition;
    }

    @Override
    public byte getByte() {
        return getByte(updatePos(1));
    }

    @Override
    public boolean getBoolean() {
        return getBoolean(updatePos(1));
    }

    @Override
    public short getShort() {
        return getShort(updatePos(2));
    }

    @Override
    public int getInt() {
        return getInt(updatePos(4));
    }

    @Override
    public long getLong() {
        return getLong(updatePos(8));
    }

    @Override
    public char getChar() {
        return getChar(updatePos(2));
    }

    @Override
    public float getFloat() {
        return getFloat(updatePos(4));
    }

    @Override
    public double getDouble() {
        return getDouble(updatePos(8));
    }

    //------

    public byte[] getBytes(int length) {
        byte[] result = super.getBytes(position,length);
        position += length*BYTE_LEN;
        return result;
    }

    public short[] getShorts(int length) {
        short[] result = super.getShorts(position,length);
        position += length*SHORT_LEN;
        return result;
    }

    public int[] getInts(int length) {
        int[] result = super.getInts(position,length);
        position += length*INT_LEN;
        return result;
    }

    public long[] getLongs(int length) {
        long[] result = super.getLongs(position,length);
        position += length*LONG_LEN;
        return result;
    }

    public char[] getChars(int length) {
        char[] result = super.getChars(position,length);
        position += length*CHAR_LEN;
        return result;
    }

    public float[] getFloats(int length) {
        float[] result = super.getFloats(position,length);
        position += length*FLOAT_LEN;
        return result;
    }

    public double[] getDoubles(int length) {
        double[] result = super.getDoubles(position,length);
        position += length*DOUBLE_LEN;
        return result;
    }

    @Override
    public<T> T asRelative(final Factory<T> factory) {
        if (position==0) return as(factory);
        else {
            return as((array, offset, limit) -> factory.get(array,offset+position,limit));
        }
    }

    @Override
    public ReadBuffer subrange(int length, boolean invert) {
        return super.subrange(position,length,invert).asReadBuffer();
    }


}
