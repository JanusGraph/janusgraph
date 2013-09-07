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

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== boolean, byte, short, int, long ======
     */

    @Override
    public void verifyAttribute(Byte value) {
        //All values are valid
    }

    @Override
    public Byte convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (Double.isNaN(d) || Math.round(d)!=d) throw new IllegalArgumentException("Not a valid byte: " + value);
            long l = ((Number)value).longValue();
            if (l>=Byte.MIN_VALUE && l<=Byte.MAX_VALUE) return Byte.valueOf((byte)l);
            else throw new IllegalArgumentException("Value too large for byte: " + value);
        } else if (value instanceof String) {
            return Byte.parseByte((String)value);
        } else return null;
    }

}
