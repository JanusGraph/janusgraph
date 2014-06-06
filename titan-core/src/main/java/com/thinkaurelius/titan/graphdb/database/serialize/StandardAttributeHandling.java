package com.thinkaurelius.titan.graphdb.database.serialize;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.*;
import com.thinkaurelius.titan.core.schema.*;
import com.thinkaurelius.titan.graphdb.database.log.LogTxStatus;
import com.thinkaurelius.titan.graphdb.database.management.MgmtLogType;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.*;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.types.ParameterType;
import com.thinkaurelius.titan.graphdb.types.SchemaStatus;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionDescription;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardTimepoint;
import com.tinkerpop.blueprints.Direction;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardAttributeHandling implements AttributeHandling {

    public static final List<Class<? extends Object>> DEFAULT_REGISTRATIONS =
            ImmutableList.of(
                    //General
                    ArrayList.class, HashMap.class, Object.class,
                    //Titan specific
                    TypeDefinitionCategory.class, TypeDefinitionDescription.class, TitanSchemaCategory.class,
                    Parameter.class, Parameter[].class, ParameterType.class,
                    Order.class, Multiplicity.class, Cardinality.class, Direction.class, ElementCategory.class,
                    ConsistencyModifier.class, SchemaStatus.class, LogTxStatus.class, MgmtLogType.class,
                    StandardDuration.class, StandardTimepoint.class
            );

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
        registerClass(Date.class, new DateSerializer());

        registerClass(Geoshape.class, new Geoshape.GeoshapeSerializer());
        registerClass(String.class, new StringSerializer()); //supports null serialization
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
