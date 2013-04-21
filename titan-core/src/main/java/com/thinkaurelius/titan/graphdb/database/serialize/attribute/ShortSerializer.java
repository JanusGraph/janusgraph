package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.nio.ByteBuffer;

public class ShortSerializer implements AttributeSerializer<Short> {

    private static final long serialVersionUID = 117423419862504186L;

    @Override
    public Short read(ByteBuffer buffer) {
        return Short.valueOf((short)(buffer.getShort() + Short.MIN_VALUE));
    }

    @Override
    public void writeObjectData(DataOutput out, Short object) {
        out.putShort((short)(object.shortValue() - Short.MIN_VALUE));
    }

}
