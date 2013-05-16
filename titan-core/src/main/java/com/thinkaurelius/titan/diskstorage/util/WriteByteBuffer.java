package com.thinkaurelius.titan.diskstorage.util;

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
    public StaticBuffer getStaticBuffer() {
        ByteBuffer b = buffer.duplicate();
        b.flip();
        return new StaticByteBuffer(b);
    }
}
