package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.DoubleSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.FloatSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.IntegerSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.LongSerializer;
import com.thinkaurelius.titan.graphdb.types.Directionality;
import com.thinkaurelius.titan.graphdb.types.FunctionalType;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabel;
import com.thinkaurelius.titan.graphdb.types.StandardPropertyKey;
import com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import com.thinkaurelius.titan.graphdb.types.TypeCategory;
import com.thinkaurelius.titan.graphdb.types.TypeVisibility;
import com.thinkaurelius.titan.graphdb.types.group.StandardTypeGroup;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class SerializerInitialization {

    public static final void initialize(Serializer serializer) {
        serializer.registerClass(StandardPropertyKey.class);
        serializer.registerClass(StandardEdgeLabel.class);
        serializer.registerClass(TypeVisibility.class);
        serializer.registerClass(Directionality.class);
        serializer.registerClass(TypeCategory.class);
        serializer.registerClass(FunctionalType.class);
        serializer.registerClass(StandardTypeGroup.class);
        serializer.registerClass(Object.class);
        serializer.registerClass(Date.class,new DateSerializer());
        serializer.registerClass(ArrayList.class);
        serializer.registerClass(HashMap.class);
        serializer.registerClass(int[].class);
        serializer.registerClass(double[].class);
        serializer.registerClass(long[].class);
        serializer.registerClass(byte[].class);
        serializer.registerClass(TitanTypeClass.class);
        serializer.registerClass(Integer.class, new IntegerSerializer());
        serializer.registerClass(Double.class, new DoubleSerializer());
        serializer.registerClass(Float.class, new FloatSerializer());
        serializer.registerClass(Long.class, new LongSerializer());
    }

    public static class DateSerializer implements AttributeSerializer<Date> {

        private final LongSerializer ls = new LongSerializer();

        @Override
        public Date read(ByteBuffer buffer) {
            return new Date(-ls.read(buffer));
        }

        @Override
        public void writeObjectData(ByteBuffer buffer, Date attribute) {
            ls.writeObjectData(buffer,-attribute.getTime());
        }
    }

}
