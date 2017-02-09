package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SerializerInjected;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.util.HashMap;

public class SerializableSerializer<T extends Serializable> implements AttributeSerializer<T>, SerializerInjected {

    private Serializer serializer;

    @Override
    public T read(ScanBuffer buffer) {
        byte[] data = serializer.readObjectNotNull(buffer,byte[].class);
        return (T) SerializationUtils.deserialize(data);
    }

    @Override
    public void write(WriteBuffer buffer, T attribute) {
        DataOutput out = (DataOutput) buffer;
        out.writeObjectNotNull(SerializationUtils.serialize(attribute));
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

}
