package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serialize.ClassSerializer;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SerializerInitialization;

import java.nio.ByteBuffer;

public class KryoSerializer extends Kryo implements Serializer {

	public KryoSerializer(boolean allowAllSerializable) {
		setRegistrationOptional(allowAllSerializable);
		register(Class.class,new ClassSerializer(this));
		registerClass(String[].class);
		SerializerInitialization.initialize(this);
	}
	
	@Override
	public<T> void registerClass(Class<T> type) {
		super.register(type);
	}
	
	@Override
	public<T> void registerClass(Class<T> type, AttributeSerializer<T> serializer) {
		super.register(type,new KryoAttributeSerializerAdapter<T>(serializer));
	}
	
//	public Object readClassAndObject(ByteBuffer buffer) {
//		return super.readClassAndObject(buffer);
//	}
// 	
//	public<T> T readObject(ByteBuffer buffer, Class<T> type) {
//		return super.readObject(buffer, type);
//	}
//	
	public<T> T readObjectNotNull(ByteBuffer buffer, Class<T> type) {
		return super.readObjectData(buffer, type);
	}
	
	@Override
	public DataOutput getDataOutput(int capacity, boolean serializeObjects) {
		if (serializeObjects) return new KryoDataOutput(capacity,this);
		else return new KryoDataOutput(capacity);
	}

	
}
