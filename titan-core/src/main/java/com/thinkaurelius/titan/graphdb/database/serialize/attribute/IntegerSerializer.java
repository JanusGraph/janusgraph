package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class IntegerSerializer implements AttributeSerializer<Integer> {

    private static final long serialVersionUID = 1174998819862504186L;

    @Override
    public Integer read(ScanBuffer buffer) {
        return Integer.valueOf(buffer.getInt() + Integer.MIN_VALUE);
    }

    @Override
    public void writeObjectData(WriteBuffer out, Integer object) {
        out.putInt(object.intValue() - Integer.MIN_VALUE);
    }

}
