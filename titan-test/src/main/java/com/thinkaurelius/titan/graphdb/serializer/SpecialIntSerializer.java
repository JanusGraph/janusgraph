package com.thinkaurelius.titan.graphdb.serializer;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.nio.ByteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SpecialIntSerializer implements AttributeSerializer<SpecialInt> {

    @Override
    public SpecialInt read(ByteBuffer buffer) {
        return new SpecialInt(buffer.getInt());
    }

    @Override
    public void writeObjectData(DataOutput out, SpecialInt attribute) {
        out.putInt(attribute.getValue());
    }
}