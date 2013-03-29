package com.thinkaurelius.titan.graphdb.database.serialize;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.*;
import com.thinkaurelius.titan.graphdb.types.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class SerializerInitialization {

    private static final int KRYO_OFFSET = 40;
    public static final int RESERVED_ID_OFFSET = 256;

    public static final void initialize(Serializer serializer) {
        serializer.registerClass(String[].class,KRYO_OFFSET+1);
        serializer.registerClass(StandardKeyDefinition.class,KRYO_OFFSET+2);
        serializer.registerClass(StandardLabelDefinition.class,KRYO_OFFSET+3);
        serializer.registerClass(StandardTypeGroup.class,KRYO_OFFSET+4);
        serializer.registerClass(Date.class, new DateSerializer(),KRYO_OFFSET+6);
        serializer.registerClass(ArrayList.class,KRYO_OFFSET+7);
        serializer.registerClass(HashMap.class,KRYO_OFFSET+8);
        serializer.registerClass(int[].class,KRYO_OFFSET+9);
        serializer.registerClass(double[].class,KRYO_OFFSET+10);
        serializer.registerClass(long[].class,KRYO_OFFSET+11);
        serializer.registerClass(byte[].class,KRYO_OFFSET+12);
        serializer.registerClass(boolean[].class,KRYO_OFFSET+13);
        serializer.registerClass(IndexType.class,KRYO_OFFSET+14);
        serializer.registerClass(TitanTypeClass.class,KRYO_OFFSET+15);
        serializer.registerClass(Integer.class, new IntegerSerializer(),KRYO_OFFSET+16);
        serializer.registerClass(Double.class, new DoubleSerializer(),KRYO_OFFSET+17);
        serializer.registerClass(Float.class, new FloatSerializer(),KRYO_OFFSET+18);
        serializer.registerClass(Long.class, new LongSerializer(),KRYO_OFFSET+19);
        serializer.registerClass(IndexType.class,KRYO_OFFSET+20);
        serializer.registerClass(IndexType[].class,KRYO_OFFSET+21);
        Preconditions.checkArgument(KRYO_OFFSET+21<RESERVED_ID_OFFSET,"ID allocation overflow!");
    }

}
