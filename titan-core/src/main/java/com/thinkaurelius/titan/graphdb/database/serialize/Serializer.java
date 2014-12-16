package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;

import java.io.Closeable;

public interface Serializer extends AttributeHandling, Closeable {

    public Object readClassAndObject(ReadBuffer buffer);

    public <T> T readObject(ReadBuffer buffer, Class<T> type);

    public <T> T readObjectByteOrder(ReadBuffer buffer, Class<T> type);

    public <T> T readObjectNotNull(ReadBuffer buffer, Class<T> type);

    public DataOutput getDataOutput(int initialCapacity);

}
