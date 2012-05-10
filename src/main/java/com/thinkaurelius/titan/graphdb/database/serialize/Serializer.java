package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.core.AttributeSerializer;

import java.nio.ByteBuffer;

public interface Serializer {

	public<T> void registerClass(Class<T> type);
	
	public<T> void registerClass(Class<T> type, AttributeSerializer<T> serializer);
	
	public Object readClassAndObject(ByteBuffer buffer);
 	
	public<T> T readObject(ByteBuffer buffer, Class<T> type);
	
	public<T> T readObjectNotNull(ByteBuffer buffer, Class<T> type);
	
	public DataOutput getDataOutput(int capacity, boolean serializeObjects);
	
}
