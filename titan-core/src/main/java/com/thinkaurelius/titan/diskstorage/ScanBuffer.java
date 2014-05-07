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

    public boolean getBoolean();

    public short getShort();

    public int getInt();

    public long getLong();

    public char getChar();

    public float getFloat();

    public double getDouble();

    public byte[] getBytes(int length);

    public short[] getShorts(int length);

    public int[] getInts(int length);

    public long[] getLongs(int length);

    public char[] getChars(int length);

    public float[] getFloats(int length);

    public double[] getDoubles(int length);

}
