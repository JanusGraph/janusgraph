package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.nio.ByteBuffer;

public class BooleanSerializer implements AttributeSerializer<Boolean> {

    @Override
    public Boolean read(ByteBuffer buffer) {
        byte s = buffer.get();
        if (s==0) return Boolean.FALSE;
        else if (s==1) return Boolean.TRUE;
        else throw new IllegalArgumentException("Invalid boolean value: " + s);
    }

    @Override
    public void writeObjectData(DataOutput out, Boolean attribute) {
        out.putByte((byte)(attribute.booleanValue()?1:0));
    }

}
