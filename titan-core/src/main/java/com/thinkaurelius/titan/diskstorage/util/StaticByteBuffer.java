package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

import java.nio.ByteBuffer;

/**
 * Implementation of {@link com.thinkaurelius.titan.diskstorage.StaticBuffer} against a {@link java.nio.ByteBuffer}
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StaticByteBuffer implements StaticBuffer {

    private final ByteBuffer b;

    /**
     * Create a {@code StaticByteBuffer} backed by an existing
     * {@code ByteBuffer}. The {@code ByteBuffer} is not copied. If
     * {@code buffer} is externally modified, then those modifications will also
     * appear in calls to the constructed {@code StaticByteBuffer}.
     *
     * @param buffer
     */
    public StaticByteBuffer(final ByteBuffer buffer) {
        Preconditions.checkNotNull(buffer);
        this.b = buffer;
    }

    public StaticByteBuffer(final byte[] bytes) {
        this(ByteBuffer.wrap(bytes));
    }

    public StaticByteBuffer(final StaticByteBuffer buffer) {
        this(buffer.b);
    }


    protected final int pos(int position) {
        return b.position() + position;
    }

    @Override
    public int length() {
        return b.limit() - b.position();
    }

    @Override
    public byte getByte(int position) {
        return b.get(pos(position));
    }

    @Override
    public short getShort(int position) {
        return b.getShort(pos(position));
    }

    @Override
    public int getInt(int position) {
        return b.getInt(pos(position));
    }

    @Override
    public long getLong(int position) {
        return b.getLong(pos(position));
    }

    @Override
    public char getChar(int position) {
        return b.getChar(pos(position));
    }

    @Override
    public float getFloat(int position) {
        return b.getFloat(pos(position));
    }

    @Override
    public double getDouble(int position) {
        return b.getDouble(pos(position));
    }

    @Override
    public StaticBuffer subrange(int position, int length) {
        Preconditions.checkArgument(position >= 0);
        Preconditions.checkArgument(length >= 0);
        Preconditions.checkArgument(b.position() + position + length <= b.limit(), "%s + %s + %s <= %s", b.position(), position, length, b.limit());
        ByteBuffer newb = b.duplicate();
        newb.position(b.position() + position);
        newb.limit(b.position() + position + length);
        return new StaticByteBuffer(newb);
    }

    @Override
    public ReadByteBuffer asReadBuffer() {
        return new ReadByteBuffer(b);
    }

    @Override
    public ByteBuffer asByteBuffer() {
        return b.duplicate();
    }

    @Override
    public <T> T as(Factory<T> factory) {
        if (b.hasArray()) {
            return factory.get(b.array(), b.position() + b.arrayOffset(), b.limit() + b.arrayOffset());
        } else {
            int len = length();
            byte[] result = new byte[len];
            for (int i = 0; i < len; i++) {
                result[i] = getByte(i);
            }
            return factory.get(result, 0, result.length);
        }
    }

    /*
    ############## IDENTICAL CODE ################
     */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        else if (o == null) return false;
        else if (!(o instanceof StaticBuffer)) return false;
        return ByteBufferUtil.equals(this, (StaticBuffer) o);
    }

    @Override
    public int hashCode() {
        return ByteBufferUtil.hashcode(this);
    }

    @Override
    public String toString() {
        return ByteBufferUtil.toString(this, "-");
    }

    @Override
    public int compareTo(StaticBuffer other) {
        return ByteBufferUtil.compare(this, other);
    }
}
