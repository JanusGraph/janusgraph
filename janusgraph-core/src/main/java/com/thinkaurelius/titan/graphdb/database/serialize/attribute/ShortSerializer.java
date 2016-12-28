package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.OrderPreservingSerializer;

public class ShortSerializer implements OrderPreservingSerializer<Short>  {

    private static final long serialVersionUID = 117423419862504186L;

    @Override
    public Short read(ScanBuffer buffer) {
        return Short.valueOf((short)(buffer.getShort() + Short.MIN_VALUE));
    }

    @Override
    public void write(WriteBuffer out, Short object) {
        out.putShort((short)(object.shortValue() - Short.MIN_VALUE));
    }

    @Override
    public Short readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Short attribute) {
        write(buffer,attribute);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== byte, short, int, long ======
     */

    @Override
    public Short convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (Double.isNaN(d) || Math.round(d)!=d) throw new IllegalArgumentException("Not a valid short: " + value);
            long l = ((Number)value).longValue();
            if (l>=Short.MIN_VALUE && l<=Short.MAX_VALUE) return Short.valueOf((short)l);
            else throw new IllegalArgumentException("Value too large for short: " + value);
        } else if (value instanceof String) {
            return Short.parseShort((String)value);
        } else return null;
    }
}
