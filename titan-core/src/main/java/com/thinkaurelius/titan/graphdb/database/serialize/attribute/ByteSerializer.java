package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class ByteSerializer implements AttributeSerializer<Byte> {

    private static final long serialVersionUID = 117423419883604186L;

    @Override
    public Byte read(ScanBuffer buffer) {
        return Byte.valueOf((byte)(buffer.getByte() + Byte.MIN_VALUE));
    }

    @Override
    public void writeObjectData(WriteBuffer out, Byte object) {
        out.putByte((byte)(object.byteValue() - Byte.MIN_VALUE));
    }

}
