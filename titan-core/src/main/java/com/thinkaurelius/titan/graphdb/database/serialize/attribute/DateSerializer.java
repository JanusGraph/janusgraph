package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;

import java.nio.ByteBuffer;
import java.util.Date;

public class DateSerializer implements AttributeSerializer<Date> {

    private final LongSerializer ls = new LongSerializer();

    @Override
    public Date read(ByteBuffer buffer) {
        long utc = -ls.read(buffer);
        Date d = new Date(utc);
        return d;
    }

    @Override
    public void writeObjectData(ByteBuffer buffer, Date attribute) {
        long utc = attribute.getTime();
        ls.writeObjectData(buffer,-utc);
    }

}
