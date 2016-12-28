package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

import java.lang.reflect.Array;

public class LongArraySerializer extends ArraySerializer implements AttributeSerializer<long[]> {

    @Override
    public long[] convert(Object value) {
        return convertInternal(value, long.class, Long.class);
    }

    @Override
    protected Object getArray(int length) {
        return new long[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setLong(array,pos,((Long)value));
    }

    //############### Serialization ###################

    @Override
    public long[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        return buffer.getLongs(length);
    }

    @Override
    public void write(WriteBuffer buffer, long[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (int i = 0; i < attribute.length; i++) buffer.putLong(attribute[i]);
    }
}