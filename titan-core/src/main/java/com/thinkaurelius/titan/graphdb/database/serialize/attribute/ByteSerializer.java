package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.nio.ByteBuffer;

public class ByteSerializer implements AttributeSerializer<Byte> {

    private static final long serialVersionUID = 117423419883604186L;

    @Override
    public Byte read(ByteBuffer buffer) {
        return Byte.valueOf((byte)(buffer.get() + Byte.MIN_VALUE));
    }

    @Override
    public void writeObjectData(DataOutput out, Byte object) {
        out.putByte((byte)(object.byteValue() - Byte.MIN_VALUE));
    }

}
