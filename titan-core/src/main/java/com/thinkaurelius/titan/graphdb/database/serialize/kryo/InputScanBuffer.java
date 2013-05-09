package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.io.Input;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InputScanBuffer implements ScanBuffer {

    private final Input input;

    public InputScanBuffer(Input input) {
        this.input = input;
    }

    @Override
    public boolean hasRemaining() {
        return input.position()<input.limit();
    }

    @Override
    public byte getByte() {
        return input.readByte();
    }

    @Override
    public short getShort() {
        return input.readShort();
    }

    @Override
    public int getInt() {
        return input.readInt();
    }

    @Override
    public long getLong() {
        return input.readLong();
    }

    @Override
    public char getChar() {
        return input.readChar();
    }

    @Override
    public float getFloat() {
        return input.readFloat();
    }

    @Override
    public double getDouble() {
        return input.readDouble();
    }

}
