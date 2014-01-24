package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class BooleanSerializer implements AttributeSerializer<Boolean> {

    @Override
    public Boolean read(ScanBuffer buffer) {
        return decode(buffer.getByte());
    }

    @Override
    public void writeObjectData(WriteBuffer out, Boolean attribute) {
        out.putByte(encode(attribute.booleanValue()));
    }

    @Override
    public void verifyAttribute(Boolean value) {
        //All values are valid
    }

    @Override
    public Boolean convert(Object value) {
        if (value instanceof Number) {
            return decode(((Number)value).byteValue());
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String)value);
        } else return null;
    }

    public static boolean decode(byte b) {
        switch (b) {
            case 0: return false;
            case 1: return true;
            default: throw new IllegalArgumentException("Invalid boolean value: " + b);
        }
    }

    public static byte encode(boolean b) {
        return (byte)(b?1:0);
    }
}
