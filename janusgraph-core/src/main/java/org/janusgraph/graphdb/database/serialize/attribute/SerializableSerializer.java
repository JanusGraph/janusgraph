package org.janusgraph.graphdb.database.serialize.attribute;

import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.SerializerInjected;
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
