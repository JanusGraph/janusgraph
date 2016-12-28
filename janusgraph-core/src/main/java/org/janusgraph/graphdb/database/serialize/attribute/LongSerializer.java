package org.janusgraph.graphdb.database.serialize.attribute;

import org.janusgraph.core.Idfiable;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.OrderPreservingSerializer;

public class LongSerializer implements OrderPreservingSerializer<Long> {

    private static final long serialVersionUID = -8438674418838450877L;

    public static final LongSerializer INSTANCE = new LongSerializer();

    @Override
    public Long read(ScanBuffer buffer) {
        return buffer.getLong() + Long.MIN_VALUE;
    }

    @Override
    public void write(WriteBuffer out, Long attribute) {
        out.putLong(attribute - Long.MIN_VALUE);
    }

    @Override
    public Long readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer out, Long attribute) {
        write(out,attribute);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== byte, short, int, long ======
     */

    @Override
    public Long convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (Double.isNaN(d) || Math.round(d)!=d) throw new IllegalArgumentException("Not a valid long: " + value);
            return ((Number)value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String)value);
        } else if (value instanceof Idfiable) {
            return ((Idfiable)value).longId();
        } else return null;
    }


}
