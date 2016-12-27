package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

import java.lang.reflect.Array;

public class DoubleArraySerializer extends ArraySerializer implements AttributeSerializer<double[]> {

    @Override
    public double[] convert(Object value) {
        return convertInternal(value, double.class, Double.class);
    }

    @Override
    protected Object getArray(int length) {
        return new double[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setDouble(array,pos,((Double)value));
    }

    //############### Serialization ###################

    @Override
    public double[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        return buffer.getDoubles(length);
    }

    @Override
    public void write(WriteBuffer buffer, double[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (int i = 0; i < attribute.length; i++) buffer.putDouble(attribute[i]);
    }
}