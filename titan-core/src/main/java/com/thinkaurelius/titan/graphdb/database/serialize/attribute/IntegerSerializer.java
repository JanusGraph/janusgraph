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

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== byte, short, int, long ======
     */

    @Override
    public void verifyAttribute(Integer value) {
        //All values are valid
    }

    @Override
    public Integer convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (Double.isNaN(d) || Math.round(d)!=d) throw new IllegalArgumentException("Not a valid integer: " + value);
            long l = ((Number)value).longValue();
            if (l>=Integer.MIN_VALUE && l<=Integer.MAX_VALUE) return Integer.valueOf((int)l);
            else throw new IllegalArgumentException("Value too large for integer: " + value);
        } else if (value instanceof String) {
            return Integer.parseInt((String)value);
        } else return null;
    }
}
