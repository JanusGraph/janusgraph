package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;

public class ByteArraySerializer extends ArraySerializer implements AttributeSerializer<byte[]> {

    @Override
    public void verifyAttribute(byte[] value) {
        //All values are valid
    }

    @Override
    public byte[] convert(Object value) {
        return convertInternal(value, byte.class, Byte.class);
    }

    @Override
    protected Object getArray(int length) {
        return new byte[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setByte(array,pos,((Byte)value));
    }

    //############### Serialization ###################

    @Override
    public byte[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        return buffer.getBytes(length);
    }

    @Override
    public void writeObjectData(WriteBuffer buffer, byte[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (int i = 0; i < attribute.length; i++) buffer.putByte(attribute[i]);
    }
}