package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;

import java.nio.ByteBuffer;

/**
 * Implementation of {@link com.thinkaurelius.titan.diskstorage.ReadBuffer} against a {@link ByteBuffer}
 *
 * Note, that the position does not impact the state of the object. Meaning, equals, hashcode,
 * and compare ignore the position.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ReadByteBuffer extends StaticByteBuffer implements ReadBuffer {

    public ReadByteBuffer(final ByteBuffer buffer) {
        super(buffer);
    }

    public ReadByteBuffer(final byte[] bytes) {
        super(bytes);
    }

    public ReadByteBuffer(final ReadByteBuffer buffer) {
        super(buffer);
        this.position=buffer.position;
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
    public void movePosition(int delta) {
        int newPosition = position + delta;
        assert newPosition >= -1 && newPosition <= length();
        this.position = newPosition;
    }

    @Override
    public byte getByte() {
        return super.getByte(updatePos(1));
    }

    @Override
    public short getShort() {
        return super.getShort(updatePos(2));
    }

    @Override
    public int getInt() {
        return super.getInt(updatePos(4));
    }

    @Override
    public long getLong() {
        return super.getLong(updatePos(8));
    }

    @Override
    public char getChar() {
        return super.getChar(updatePos(2));
    }

    @Override
    public float getFloat() {
        return super.getFloat(updatePos(4));
    }

    @Override
    public double getDouble() {
        return super.getDouble(updatePos(8));
    }

    @Override
    public ByteBuffer asRelativeByteBuffer() {
        ByteBuffer rb = super.asByteBuffer();
        rb.position(rb.position()+position);
        return rb;
    }

    @Override
    public<T> T asRelative(final Factory<T> factory) {
        if (position==0) return super.as(factory);
        else {
            return super.as(new Factory<T>() {
                @Override
                public T get(byte[] array, int offset, int limit) {
                    return factory.get(array,offset+position,limit);
                }
            });
        }
    }

    @Override
    public ReadBuffer invert() {
        byte[] newvalues = new byte[super.length()];
        for (int i=0;i<newvalues.length;i++) newvalues[i]=(byte)~super.getByte(i);
        ReadArrayBuffer newread = new ReadArrayBuffer(newvalues);
        newread.movePosition(this.position);
        return newread;
    }

}
