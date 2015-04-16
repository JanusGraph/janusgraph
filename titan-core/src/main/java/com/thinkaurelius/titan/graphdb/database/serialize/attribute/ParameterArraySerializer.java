package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SerializerInjected;

import java.lang.reflect.Array;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ParameterArraySerializer extends ArraySerializer implements AttributeSerializer<Parameter[]>, SerializerInjected {

    private Serializer serializer;

    @Override
    public Parameter[] convert(Object value) {
        return convertInternal(value, null, Parameter.class);
    }

    @Override
    protected Object getArray(int length) {
        return new Parameter[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.set(array, pos, ((Parameter) value));
    }

    //############### Serialization ###################

    @Override
    public Parameter[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        Parameter[] result = new Parameter[length];
        for (int i = 0; i < length; i++) {
            result[i]=serializer.readObjectNotNull(buffer,Parameter.class);
        }
        return result;
    }

    @Override
    public void write(WriteBuffer buffer, Parameter[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (int i = 0; i < attribute.length; i++) ((DataOutput)buffer).writeObjectNotNull(attribute[i]);
    }


    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer=serializer;
    }
}
