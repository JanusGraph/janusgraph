package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

import java.lang.reflect.Array;

public class StringArraySerializer extends ArraySerializer implements AttributeSerializer<String[]> {

    private static final StringSerializer stringSerializer = new StringSerializer();

    @Override
    public String[] convert(Object value) {
        return convertInternal(value, null, String.class);
    }

    @Override
    protected Object getArray(int length) {
        return new String[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.set(array, pos, ((String) value));
    }

    //############### Serialization ###################

    @Override
    public String[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        String[] result = new String[length];
        for (int i = 0; i < length; i++) {
            result[i]=stringSerializer.read(buffer);
        }
        return result;
    }

    @Override
    public void write(WriteBuffer buffer, String[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (int i = 0; i < attribute.length; i++) stringSerializer.write(buffer, attribute[i]);
    }
}