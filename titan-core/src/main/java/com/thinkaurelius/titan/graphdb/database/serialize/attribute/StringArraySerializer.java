package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

import java.lang.reflect.Array;

public class StringArraySerializer extends ArraySerializer implements AttributeSerializer<String[]> {

    //TODO: use StringX here
    private static final StringSerializer stringSerializer = new StringSerializer();

    @Override
    public void verifyAttribute(String[] value) {
        //All values are valid
    }

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
    public void writeObjectData(WriteBuffer buffer, String[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (int i = 0; i < attribute.length; i++) stringSerializer.writeObjectData(buffer,attribute[i]);
    }
}