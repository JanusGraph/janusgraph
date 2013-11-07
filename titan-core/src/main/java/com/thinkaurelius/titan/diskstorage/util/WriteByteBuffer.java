package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class WriteByteBuffer implements WriteBuffer {

    private final ByteBuffer buffer;

    public WriteByteBuffer(int capacity) {
        buffer = ByteBuffer.allocate(capacity);
    }

    @Override
    public WriteBuffer putLong(long val) {
        buffer.putLong(val);
        return this;
    }

    @Override
    public WriteBuffer putInt(int val) {
        buffer.putInt(val);
        return this;
    }

    @Override
    public WriteBuffer putShort(short val) {
        buffer.putShort(val);
        return this;
    }

    @Override
    public WriteBuffer putByte(byte val) {
        buffer.put(val);
        return this;
    }

    @Override
    public WriteBuffer putChar(char val) {
        buffer.putChar(val);
        return this;
    }

    @Override
    public WriteBuffer putFloat(float val) {
        buffer.putFloat(val);
        return this;
    }

    @Override
    public WriteBuffer putDouble(double val) {
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
        return new StaticByteBuffer(b);
    }
}
