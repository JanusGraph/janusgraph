package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.types.*;
import com.tinkerpop.blueprints.Direction;

import java.lang.reflect.Constructor;
import java.util.*;

public class KryoSerializer {

    public static final int DEFAULT_MAX_OUTPUT_SIZE = 10 * 1024 * 1024; // 10 MB in bytes
    public static final int KRYO_ID_OFFSET = 50;

    private final boolean registerRequired;
    private final ThreadLocal<Kryo> kryos;
    private final Map<Integer,TypeRegistration> registrations;
    private final int maxOutputSize;

    private static final StaticBuffer.Factory<Input> INPUT_FACTORY = new StaticBuffer.Factory<Input>() {
        @Override
        public Input get(byte[] array, int offset, int limit) {
            //Needs to copy array - otherwise we see BufferUnderflow exceptions from concurrent access
            //See https://github.com/EsotericSoftware/kryo#threading
            return new Input(Arrays.copyOfRange(array,offset,limit));
        }
    };

    public KryoSerializer(final List<Class> defaultRegistrations) {
        this(defaultRegistrations, false);
    }


    public KryoSerializer(final List<Class> defaultRegistrations, boolean registrationRequired) {
        this(defaultRegistrations, registrationRequired, DEFAULT_MAX_OUTPUT_SIZE);
    }

    public KryoSerializer(final List<Class> defaultRegistrations, boolean registrationRequired, int maxOutputSize) {
        this.maxOutputSize = maxOutputSize;
        this.registerRequired = registrationRequired;
        this.registrations = new HashMap<Integer,TypeRegistration>();

        for (Class clazz : defaultRegistrations) {
//            Preconditions.checkArgument(isValidClass(clazz),"Class does not have a default constructor: %s",clazz.getName());
            objectVerificationCache.put(clazz,Boolean.TRUE);
        }

        kryos = new ThreadLocal<Kryo>() {
            public Kryo initialValue() {
                Kryo k = new Kryo();
                k.setRegistrationRequired(registerRequired);
                k.register(Class.class,new DefaultSerializers.ClassSerializer());
                for (int i=0;i<defaultRegistrations.size();i++) {
                    Class clazz = defaultRegistrations.get(i);
                    k.register(clazz, KRYO_ID_OFFSET + i);
                }
                return k;
            }
        };
    }

    Kryo getKryo() {
        return kryos.get();
    }

    public Object readClassAndObject(ReadBuffer buffer) {
        Input i = buffer.asRelative(INPUT_FACTORY);
        int startPos = i.position();
        Object value = getKryo().readClassAndObject(i);
        buffer.movePositionTo(buffer.getPosition()+i.position()-startPos);
        return value;
    }

//    public <T> T readObject(ReadBuffer buffer, Class<T> type) {
//        Input i = buffer.asRelative(INPUT_FACTORY);
//        int startPos = i.position();
//        T value = getKryo().readObjectOrNull(i, type);
//        buffer.movePositionTo(buffer.getPosition()+i.position()-startPos);
//        return value;
//    }

    public <T> T readObjectNotNull(ReadBuffer buffer, Class<T> type) {
        Input i = buffer.asRelative(INPUT_FACTORY);
        int startPos = i.position();
        T value = getKryo().readObject(i, type);
        buffer.movePositionTo(buffer.getPosition()+i.position()-startPos);
        return value;
    }

    private Output getOutput(Object object) {
        return new Output(128,maxOutputSize);
    }

    private void writeOutput(WriteBuffer out, Output output) {
        byte[] array = output.getBuffer();
        int limit = output.position();
        for (int i=0;i<limit;i++) out.putByte(array[i]);
    }

//    public void writeObject(WriteBuffer out, Object object, Class<?> type) {
//        Preconditions.checkArgument(isValidObject(object), "Cannot de-/serialize object: %s", object);
//        Output output = getOutput(object);
//        getKryo().writeObjectOrNull(output, object, type);
//        writeOutput(out,output);
//    }

    public void writeObjectNotNull(WriteBuffer out, Object object) {
        Preconditions.checkNotNull(object);
        Preconditions.checkArgument(isValidObject(object), "Cannot de-/serialize object: %s", object);
        Output output = getOutput(object);
        getKryo().writeObject(output, object);
        writeOutput(out,output);
    }

    public void writeClassAndObject(WriteBuffer out, Object object) {
        Preconditions.checkArgument(isValidObject(object), "Cannot de-/serialize object: %s", object);
        Output output = getOutput(object);
        getKryo().writeClassAndObject(output, object);
        writeOutput(out,output);
    }

    private final Cache<Class<?>,Boolean> objectVerificationCache = CacheBuilder.newBuilder()
                                .maximumSize(10000).concurrencyLevel(4).initialCapacity(32).build();

    final boolean isValidObject(final Object o) {
        if (o==null) return true;
        Boolean status = objectVerificationCache.getIfPresent(o.getClass());
        if (status==null) {
            Kryo kryo = getKryo();
            if (!(kryo.getSerializer(o.getClass()) instanceof FieldSerializer)) status=Boolean.TRUE;
            else if (!isValidClass(o.getClass())) status=Boolean.FALSE;
            else {
                try {
                    Output out = new Output(128, maxOutputSize);
                    kryo.writeClassAndObject(out,o);
                    Input in = new Input(out.getBuffer(),0,out.position());
                    Object ocopy = kryo.readClassAndObject(in);
                    status=(o.equals(ocopy)?Boolean.TRUE:Boolean.FALSE);
                } catch (Throwable e) {
                    status=Boolean.FALSE;
                }
            }
            objectVerificationCache.put(o.getClass(),status);
        }
        return status;

    }

    public static final boolean isValidClass(Class<?> type) {
        if (type.isPrimitive()) return true;
        else if (Enum.class.isAssignableFrom(type)) return true;
        else if (type.isArray()) {
            return isValidClass(type.getComponentType());
        } else {
            for (Constructor c : type.getDeclaredConstructors()) {
                if (c.getParameterTypes().length==0) return true;
            }
            return false;
        }
    }

    private static class TypeRegistration {

        final Class type;
        final com.esotericsoftware.kryo.Serializer serializer;

        TypeRegistration(Class type, com.esotericsoftware.kryo.Serializer serializer) {
            this.type=type;
            this.serializer=serializer;
        }

    }

}
