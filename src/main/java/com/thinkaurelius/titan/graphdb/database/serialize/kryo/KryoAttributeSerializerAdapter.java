package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Serializer;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeSerializer;

import java.nio.ByteBuffer;

public class KryoAttributeSerializerAdapter<T> extends Serializer {

	private final AttributeSerializer<T> serializer;
	
	KryoAttributeSerializerAdapter(AttributeSerializer<T> serializer) {
		Preconditions.checkNotNull(serializer);
		this.serializer=serializer;
	}
	
	@Override
	public T readObjectData(ByteBuffer buffer, Class type) {
		return serializer.read(buffer);
	}

	@Override
	public void writeObjectData(ByteBuffer buffer, Object att) {
		serializer.writeObjectData(buffer, (T)att);
	}

}
