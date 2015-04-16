package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

import java.lang.reflect.Array;

public class IntArraySerializer extends ArraySerializer implements AttributeSerializer<int[]> {

    @Override
    public int[] convert(Object value) {
        return convertInternal(value, int.class, Integer.class);
    }

    @Override
    protected Object getArray(int length) {
        return new int[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setInt(array,pos,((Integer)value));
    }

    //############### Serialization ###################

    @Override
    public int[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        return buffer.getInts(length);
    }

    @Override
    public void write(WriteBuffer buffer, int[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (int i = 0; i < attribute.length; i++) buffer.putInt(attribute[i]);
    }
}