package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class BooleanSerializer implements AttributeSerializer<Boolean> {

    @Override
    public Boolean read(ScanBuffer buffer) {
        byte s = buffer.getByte();
        if (s==0) return Boolean.FALSE;
        else if (s==1) return Boolean.TRUE;
        else throw new IllegalArgumentException("Invalid boolean value: " + s);
    }

    @Override
    public void writeObjectData(WriteBuffer out, Boolean attribute) {
        out.putByte((byte)(attribute.booleanValue()?1:0));
    }

    @Override
    public void verifyAttribute(Boolean value) {
        //All values are valid
    }

    @Override
    public Boolean convert(Object value) {
        if (value instanceof Number) {
            Number n = (Number)value;
            if (n.doubleValue()==1.0) return Boolean.TRUE;
            else if (n.doubleValue()==0.0) return Boolean.FALSE;
            else throw new IllegalArgumentException("Number does not map to boolean value: " + value);
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String)value);
        } else return null;
    }
}
