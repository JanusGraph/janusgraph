package com.thinkaurelius.titan.diskstorage;

/**
 * A Buffer that allows sequential reads.
 * Should not be used by multiple threads.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ScanBuffer {

    public boolean hasRemaining();

    public byte getByte();

    public short getShort();

    public int getInt();

    public long getLong();

    public char getChar();

    public float getFloat();

    public double getDouble();

}
