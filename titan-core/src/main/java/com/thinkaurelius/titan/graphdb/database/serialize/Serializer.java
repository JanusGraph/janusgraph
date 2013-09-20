package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;

public interface Serializer extends AttributeHandling {

    public <T> void registerClass(Class<T> type, int id);

    public <T> void registerClass(Class<T> type, AttributeHandler<T> handler, int id);

    public <T> void registerClass(Class<T> type, AttributeSerializer<T> serializer, int id);

    public Object readClassAndObject(ReadBuffer buffer);

    public <T> T readObject(ReadBuffer buffer, Class<T> type);

    public <T> T readObjectNotNull(ReadBuffer buffer, Class<T> type);

    public DataOutput getDataOutput(int capacity, boolean serializeObjects);

}
