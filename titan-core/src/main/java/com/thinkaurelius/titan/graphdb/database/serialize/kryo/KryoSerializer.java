package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SerializerInitialization;
import com.thinkaurelius.titan.graphdb.database.serialize.DefaultAttributeHandling;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KryoSerializer extends DefaultAttributeHandling implements Serializer {

    private static final int MAX_OUTPUT_SIZE = 10 * 1024 * 1024;

    private final boolean registerRequired;
    private final ThreadLocal<Kryo> kryos;
    private final Map<Integer,TypeRegistration> registrations;

    private static final StaticBuffer.Factory<Input> INPUT_FACTORY = new StaticBuffer.Factory<Input>() {
        @Override
        public Input get(byte[] array, int offset, int limit) {
            //Needs to copy array - otherwise we see BufferUnderflow exceptions from concurrent access
            return new Input(Arrays.copyOfRange(array,offset,limit));
        }
    };

    private boolean initialized=false;


    public KryoSerializer(final boolean allowAllSerializable) {
        this.registerRequired=!allowAllSerializable;
        this.registrations = new HashMap<Integer,TypeRegistration>();

        kryos = new ThreadLocal<Kryo>() {
            public Kryo initialValue() {
                initialized=true;
                Kryo k = new Kryo();
                k.setRegistrationRequired(registerRequired);
                k.register(Class.class,new DefaultSerializers.ClassSerializer());
                for (Map.Entry<Integer,TypeRegistration> entry : registrations.entrySet()) {
                    if (entry.getValue().serializer==null) {
                        k.register(entry.getValue().type,entry.getKey());
                    } else {
                        k.register(entry.getValue().type,entry.getValue().serializer,entry.getKey());
                    }
                }
                return k;
            }
        };
        SerializerInitialization.initialize(this);
    }

    @Override
    public synchronized  <T> void registerClass(Class<T> type, int id) {
        Preconditions.checkArgument(!initialized,"Serializer has already been initialized!");
        Preconditions.checkArgument(id>0,"Invalid id provided: %s",id);
        Preconditions.checkArgument(!registrations.containsKey(id),"ID has already been registered: %s",id);
        Preconditions.checkArgument(isValidClass(type),"Class does not have a default constructor: %s",type.getName());
        registrations.put(id,new TypeRegistration(type,null));
        objectVerificationCache.put(type,Boolean.TRUE);
    }

    @Override
    public <T> void registerClass(Class<T> type, AttributeHandler<T> handler, int id) {
        super.registerClass(type,handler);
        this.registerClass(type,id);
    }

    @Override
    public synchronized  <T> void registerClass(Class<T> type, AttributeSerializer<T> serializer, int id) {
        super.registerClass(type,serializer);
        Preconditions.checkArgument(!initialized,"Serializer has already been initialized!");
        Preconditions.checkArgument(id>0,"Invalid id provided: %s",id);
        Preconditions.checkArgument(!registrations.containsKey(id),"ID has already been registered: %s",id);
        registrations.put(id,new TypeRegistration(type,new KryoAttributeSerializerAdapter<T>(serializer)));
        objectVerificationCache.put(type,Boolean.TRUE);
    }

    Kryo getKryo() {
        return kryos.get();
    }

    @Override
    public Object readClassAndObject(ReadBuffer buffer) {
        Input i = buffer.asRelative(INPUT_FACTORY);
        int startPos = i.position();
        Object value = getKryo().readClassAndObject(i);
        buffer.movePosition(i.position()-startPos);
        return value;
    }

    @Override
    public <T> T readObject(ReadBuffer buffer, Class<T> type) {
        Input i = buffer.asRelative(INPUT_FACTORY);
        int startPos = i.position();
        T value = getKryo().readObjectOrNull(i, type);
        buffer.movePosition(i.position()-startPos);
        return value;
    }

    public <T> T readObjectNotNull(ReadBuffer buffer, Class<T> type) {
        Input i = buffer.asRelative(INPUT_FACTORY);
        int startPos = i.position();
        T value = getKryo().readObject(i, type);
        buffer.movePosition(i.position()-startPos);
        return value;
    }

    @Override
    public DataOutput getDataOutput(int capacity, boolean serializeObjects) {
        Output output = new Output(capacity,MAX_OUTPUT_SIZE);
        if (serializeObjects) return new KryoDataOutput(output, this);
        else return new KryoDataOutput(output);
    }

    private final Cache<Class<?>,Boolean> objectVerificationCache = CacheBuilder.newBuilder()
                                .maximumSize(10000).concurrencyLevel(4).initialCapacity(32).build();

    final boolean isValidObject(Kryo kryo, final Object o) {
        if (o==null) return true;
        Boolean status = objectVerificationCache.getIfPresent(o.getClass());
        if (status==null) {
            if (!(kryo.getSerializer(o.getClass()) instanceof FieldSerializer)) status=Boolean.TRUE;
            else if (!isValidClass(o.getClass())) status=Boolean.FALSE;
            else {
                try {
                    Output out = new Output(128, MAX_OUTPUT_SIZE);
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
