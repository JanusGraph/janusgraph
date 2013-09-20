package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

import java.util.Date;

public class DateSerializer implements AttributeSerializer<Date> {

    private final LongSerializer ls = new LongSerializer();

    @Override
    public Date read(ScanBuffer buffer) {
        long utc = -ls.read(buffer);
        Date d = new Date(utc);
        return d;
    }

    @Override
    public void writeObjectData(WriteBuffer out, Date attribute) {
        long utc = attribute.getTime();
        ls.writeObjectData(out,-utc);
    }

    @Override
    public void verifyAttribute(Date value) {
        //All values are valid
    }

    @Override
    public Date convert(Object value) {
        if (value instanceof Number && !(value instanceof Float) && !(value instanceof Double)) {
            return new Date(((Number)value).longValue());
        } else return null;
    }
}
