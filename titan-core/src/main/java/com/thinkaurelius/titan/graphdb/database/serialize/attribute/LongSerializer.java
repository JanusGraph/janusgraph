package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class LongSerializer implements AttributeSerializer<Long> {

    private static final long serialVersionUID = -8438674418838450877L;

    @Override
    public Long read(ScanBuffer buffer) {
        return Long.valueOf(buffer.getLong() + Long.MIN_VALUE);
    }

    @Override
    public void writeObjectData(WriteBuffer out, Long object) {
        out.putLong(object.longValue() - Long.MIN_VALUE);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== byte, short, int, long ======
     */

    @Override
    public void verifyAttribute(Long value) {
        //All values are valid
    }

    @Override
    public Long convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (Double.isNaN(d) || Math.round(d)!=d) throw new IllegalArgumentException("Not a valid long: " + value);
            long l = ((Number)value).longValue();
            return Long.valueOf(l);
        } else if (value instanceof String) {
            return Long.parseLong((String)value);
        } else return null;
    }
}
