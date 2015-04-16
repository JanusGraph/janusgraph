package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SerializerInjected;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ParameterSerializer implements AttributeSerializer<Parameter>, SerializerInjected {

    private Serializer serializer;

    @Override
    public Parameter read(ScanBuffer buffer) {
        String key = serializer.readObjectNotNull(buffer,String.class);
        Object value = serializer.readClassAndObject(buffer);
        return new Parameter(key,value);
    }

    @Override
    public void write(WriteBuffer buffer, Parameter attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute.key());
        out.writeClassAndObject(attribute.value());
    }


    @Override
    public void setSerializer(Serializer serializer) {
        Preconditions.checkNotNull(serializer);
        this.serializer=serializer;
    }
}
