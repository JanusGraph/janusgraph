package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.serialize.ClassSerializer;
import com.esotericsoftware.kryo.serialize.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SerializerInitialization;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

public class KryoSerializer extends Kryo implements Serializer {

    public KryoSerializer(boolean allowAllSerializable) {
        setRegistrationOptional(allowAllSerializable);
        register(Class.class, new ClassSerializer(this));
        registerClass(String[].class);
        SerializerInitialization.initialize(this);
    }

    @Override
    public <T> void registerClass(Class<T> type) {
        super.register(type);
    }

    @Override
    public <T> void registerClass(Class<T> type, AttributeSerializer<T> serializer) {
        super.register(type, new KryoAttributeSerializerAdapter<T>(serializer));
    }

    //	public Object readClassAndObject(ByteBuffer buffer) {
//		return super.readClassAndObject(buffer);
//	}
// 	
//	public<T> T readObject(ByteBuffer buffer, Class<T> type) {
//		return super.readObject(buffer, type);
//	}
//	
    public <T> T readObjectNotNull(ByteBuffer buffer, Class<T> type) {
        return super.readObjectData(buffer, type);
    }

    @Override
    public DataOutput getDataOutput(int capacity, boolean serializeObjects) {
        if (serializeObjects) return new KryoDataOutput(capacity, this);
        else return new KryoDataOutput(capacity);
    }

    private final Cache<Class<?>,Boolean> objectVerificationCache = CacheBuilder.newBuilder()
                                .maximumSize(10000).concurrencyLevel(4).initialCapacity(32).build();

    final boolean isValidObject(final Object o) {
        Preconditions.checkNotNull(o);
        Boolean status = objectVerificationCache.getIfPresent(o.getClass());
        if (status==null) {
            if (!(getSerializer(o.getClass()) instanceof FieldSerializer)) status=true;
            else if (!isValidClass(o.getClass())) status=false;
            else {
                try {
                    ObjectBuffer objects = new ObjectBuffer(this, 128, 100000);
                    ByteBuffer b = ByteBuffer.wrap(objects.writeClassAndObject(o));
                    Object ocopy = readClassAndObject(b);
                    status=o.equals(ocopy);
                } catch (Throwable e) {
                    status=false;
                }
            }
            objectVerificationCache.put(o.getClass(),status);
        }
        return status;

    }

    public static final boolean isValidClass(Class<?> type) {
        for (Constructor c : type.getConstructors()) {
            if (c.getParameterTypes().length==0) return true;
        }
        return false;
    }

}
