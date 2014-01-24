package com.thinkaurelius.titan.graphdb.database.serialize;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardAttributeHandling implements AttributeHandling {

    private final Map<Class,AttributeHandler> handlers;

    public StandardAttributeHandling() {
        handlers = new HashMap<Class, AttributeHandler>(60);

        //Sort key data types
        registerClass(Byte.class, new ByteSerializer());
        registerClass(Short.class, new ShortSerializer());
        registerClass(Integer.class, new IntegerSerializer());
        registerClass(Long.class, new LongSerializer());
        registerClass(Decimal.class, new Decimal.DecimalSerializer());
        registerClass(Precision.class, new Precision.PrecisionSerializer());
        registerClass(Character.class, new CharacterSerializer());
        registerClass(Boolean.class, new BooleanSerializer());
        registerClass(String.class, new StringSerializer());
        registerClass(Date.class, new DateSerializer());

        registerClass(Geoshape.class, new Geoshape.GeoshapeSerializer());
        registerClass(StringX.class, new StringXSerializer()); //supports null serialization
        registerClass(Float.class, new FloatSerializer());
        registerClass(Double.class, new DoubleSerializer());


        //Arrays (support null serialization)
        registerClass(byte[].class, new ByteArraySerializer());
        registerClass(short[].class, new ShortArraySerializer());
        registerClass(int[].class, new IntArraySerializer());
        registerClass(long[].class, new LongArraySerializer());
        registerClass(float[].class, new FloatArraySerializer());
        registerClass(double[].class, new DoubleArraySerializer());
        registerClass(char[].class, new CharArraySerializer());
        registerClass(boolean[].class, new BooleanArraySerializer());
        registerClass(String[].class, new StringArraySerializer());

    }

    @Override
    public synchronized <V> void registerClass(Class<V> datatype, AttributeHandler<V> handler) {
        Preconditions.checkNotNull(datatype);
        Preconditions.checkNotNull(handler);
        Preconditions.checkArgument(!handlers.containsKey(datatype),"DataType has already been registered: %s",datatype);
        handlers.put(datatype,handler);
    }

    @Override
    public <V> void verifyAttribute(Class<V> datatype, Object value) {
        Preconditions.checkNotNull(datatype);
        Preconditions.checkNotNull(value);
        AttributeHandler handler = handlers.get(datatype);
        if (handler!=null) handler.verifyAttribute(value);
    }

    @Override
    public <V> V convert(Class<V> datatype, Object value) {
        Preconditions.checkNotNull(datatype);
        Preconditions.checkNotNull(value);
        AttributeHandler handler = handlers.get(datatype);
        if (handler!=null) return (V)handler.convert(value);
        else return null;
    }

    @Override
    public boolean isOrderPreservingDatatype(Class<?> datatype) {
        return (getSerializer(datatype) instanceof OrderPreservingSerializer);
    }

    protected <V> AttributeSerializer<V> getSerializer(Class<V> datatype) {
        Preconditions.checkNotNull(datatype);
        AttributeHandler handler = handlers.get(datatype);
        if (handler!=null && handler instanceof AttributeSerializer) {
            return (AttributeSerializer)handler;
        } else return null;
    }
}
