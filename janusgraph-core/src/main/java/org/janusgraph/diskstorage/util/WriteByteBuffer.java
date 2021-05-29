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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

import java.nio.ByteBuffer;

import static org.janusgraph.diskstorage.util.StaticArrayBuffer.BYTE_LEN;
import static org.janusgraph.diskstorage.util.StaticArrayBuffer.CHAR_LEN;
import static org.janusgraph.diskstorage.util.StaticArrayBuffer.DOUBLE_LEN;
import static org.janusgraph.diskstorage.util.StaticArrayBuffer.FLOAT_LEN;
import static org.janusgraph.diskstorage.util.StaticArrayBuffer.INT_LEN;
import static org.janusgraph.diskstorage.util.StaticArrayBuffer.LONG_LEN;
import static org.janusgraph.diskstorage.util.StaticArrayBuffer.SHORT_LEN;


/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class WriteByteBuffer implements WriteBuffer {

    public static final int DEFAULT_CAPACITY = 64;
    public static final int MAX_BUFFER_CAPACITY = 128 * 1024 * 1024; //128 MB

    private ByteBuffer buffer;

    public WriteByteBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public WriteByteBuffer(int capacity) {
        Preconditions.checkArgument(capacity<=MAX_BUFFER_CAPACITY,"Capacity exceeds max buffer capacity: %s",MAX_BUFFER_CAPACITY);
        buffer = ByteBuffer.allocate(capacity);
    }

    private void require(int size) {
        if (buffer.capacity()-buffer.position()<size) {
            //Need to resize
            int newCapacity = buffer.position() + size + buffer.capacity(); //extra capacity as buffer
            Preconditions.checkArgument(newCapacity<=MAX_BUFFER_CAPACITY,"Capacity exceeds max buffer capacity: %s",MAX_BUFFER_CAPACITY);
            ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
            buffer.flip();
            newBuffer.put(buffer);
            buffer=newBuffer;
        }
    }

    @Override
    public WriteBuffer putLong(long val) {
        require(LONG_LEN);
        buffer.putLong(val);
        return this;
    }

    @Override
    public WriteBuffer putInt(int val) {
        require(INT_LEN);
        buffer.putInt(val);
        return this;
    }

    @Override
    public WriteBuffer putShort(short val) {
        require(SHORT_LEN);
        buffer.putShort(val);
        return this;
    }

    @Override
    public WriteBuffer putBoolean(boolean val) {
        return putByte((byte)(val?1:0));
    }

    @Override
    public WriteBuffer putByte(byte val) {
        require(BYTE_LEN);
        buffer.put(val);
        return this;
    }

    @Override
    public WriteBuffer putBytes(byte[] val) {
        require(BYTE_LEN*val.length);
        buffer.put(val);
        return this;
    }

    @Override
    public WriteBuffer putBytes(final StaticBuffer val) {
        require(BYTE_LEN*val.length());
        val.as((array, offset, limit) -> {
            buffer.put(array,offset,val.length());
            return Boolean.TRUE;
        });
        return this;
    }

    @Override
    public WriteBuffer putChar(char val) {
        require(CHAR_LEN);
        buffer.putChar(val);
        return this;
    }

    @Override
    public WriteBuffer putFloat(float val) {
        require(FLOAT_LEN);
        buffer.putFloat(val);
        return this;
    }

    @Override
    public WriteBuffer putDouble(double val) {
        require(DOUBLE_LEN);
        buffer.putDouble(val);
        return this;
    }

    @Override
    public int getPosition() {
        return buffer.position();
    }

    @Override
    public StaticBuffer getStaticBuffer() {
        return getStaticBufferFlipBytes(0,0);
    }

    @Override
    public StaticBuffer getStaticBufferFlipBytes(int from, int to) {
        ByteBuffer b = buffer.duplicate();
        b.flip();
        Preconditions.checkArgument(from>=0 && from<=to);
        Preconditions.checkArgument(to<=b.limit());
        for (int i=from;i<to;i++) b.put(i,(byte)~b.get(i));
        return StaticArrayBuffer.of(b);
    }
}
