package com.thinkaurelius.titan.diskstorage;

/**
 * A Buffer that allows simple writes and returns the result as a {@link StaticBuffer}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface WriteBuffer {

    public WriteBuffer putLong(long val);

    public WriteBuffer putInt(int val);

    public WriteBuffer putShort(short val);

    public WriteBuffer putByte(byte val);

    public WriteBuffer putChar(char val);

    public WriteBuffer putFloat(float val);

    public WriteBuffer putDouble(double val);

    public StaticBuffer getStaticBuffer();

    public int getPosition();

    public StaticBuffer getStaticBufferFlipBytes(int from, int to);

}
