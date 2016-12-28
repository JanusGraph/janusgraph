package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.OrderPreservingSerializer;

import java.util.Date;

public class DateSerializer implements OrderPreservingSerializer<Date> {

    private final LongSerializer ls = LongSerializer.INSTANCE;

    @Override
    public Date read(ScanBuffer buffer) {
        long utc = ls.read(buffer);
        Date d = new Date(utc);
        return d;
    }

    @Override
    public void write(WriteBuffer out, Date attribute) {
        long utc = attribute.getTime();
        ls.write(out, utc);
    }

    @Override
    public Date readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Date attribute) {
        write(buffer,attribute);
    }

    @Override
    public Date convert(Object value) {
        if (value instanceof Number && !(value instanceof Float) && !(value instanceof Double)) {
            return new Date(((Number)value).longValue());
        } else return null;
    }
}
