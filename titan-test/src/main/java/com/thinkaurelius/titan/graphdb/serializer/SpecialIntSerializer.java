package com.thinkaurelius.titan.graphdb.serializer;

import com.thinkaurelius.titan.core.AttributeSerializer;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class SpecialIntSerializer implements AttributeSerializer<SpecialInt> {

    @Override
    public SpecialInt read(ByteBuffer buffer) {
        return new SpecialInt(buffer.getInt());
    }

    @Override
    public void writeObjectData(ByteBuffer buffer, SpecialInt attribute) {
        buffer.putInt(attribute.getValue());
    }
}