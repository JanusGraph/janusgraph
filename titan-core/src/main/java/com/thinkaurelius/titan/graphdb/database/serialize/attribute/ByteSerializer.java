package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.OrderPreservingSerializer;

public class ByteSerializer implements OrderPreservingSerializer<Byte> {

    private static final long serialVersionUID = 117423419883604186L;

    @Override
    public Byte read(ScanBuffer buffer) {
        return Byte.valueOf((byte)(buffer.getByte() + Byte.MIN_VALUE));
    }

    @Override
    public void write(WriteBuffer out, Byte object) {
        out.putByte((byte)(object.byteValue() - Byte.MIN_VALUE));
    }

    @Override
    public Byte readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Byte attribute) {
        write(buffer,attribute);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== boolean, byte, short, int, long ======
     */

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
