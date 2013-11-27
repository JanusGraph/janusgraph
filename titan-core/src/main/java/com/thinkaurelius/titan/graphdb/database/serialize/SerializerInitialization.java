package com.thinkaurelius.titan.graphdb.database.serialize;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.attribute.FullDouble;
import com.thinkaurelius.titan.core.attribute.FullFloat;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.*;
import com.thinkaurelius.titan.graphdb.types.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class SerializerInitialization {

    private static final int KRYO_OFFSET = 40;
    public static final int RESERVED_ID_OFFSET = 256;

    public static final void initialize(Serializer serializer) {
        serializer.registerClass(String[].class, KRYO_OFFSET + 1);
        serializer.registerClass(TypeAttributeType.class, KRYO_OFFSET + 2);
        serializer.registerClass(TypeAttribute.class, KRYO_OFFSET + 3);
        serializer.registerClass(String.class, new StringSerializer(), KRYO_OFFSET + 4);
        serializer.registerClass(Date.class, new DateSerializer(), KRYO_OFFSET + 6);
        serializer.registerClass(ArrayList.class, KRYO_OFFSET + 7);
        serializer.registerClass(HashMap.class, KRYO_OFFSET + 8);
        serializer.registerClass(int[].class, KRYO_OFFSET + 9);
        serializer.registerClass(double[].class, KRYO_OFFSET + 10);
        serializer.registerClass(long[].class, KRYO_OFFSET + 11);
        serializer.registerClass(byte[].class, new ByteArrayHandler(), KRYO_OFFSET + 12);
        serializer.registerClass(boolean[].class, KRYO_OFFSET + 13);
        serializer.registerClass(IndexType.class, KRYO_OFFSET + 14); //duplicate of 20 TODO: remove one!
        serializer.registerClass(TitanTypeClass.class, KRYO_OFFSET + 15);
        serializer.registerClass(Integer.class, new IntegerSerializer(), KRYO_OFFSET + 16);
        serializer.registerClass(Double.class, new DoubleSerializer(), KRYO_OFFSET + 17);
        serializer.registerClass(Float.class, new FloatSerializer(), KRYO_OFFSET + 18);
        serializer.registerClass(Long.class, new LongSerializer(), KRYO_OFFSET + 19);
        serializer.registerClass(IndexType.class, KRYO_OFFSET + 20);
        serializer.registerClass(IndexType[].class, KRYO_OFFSET + 21);
        serializer.registerClass(Geoshape.class, new GeoshapeHandler(), KRYO_OFFSET + 22);
        serializer.registerClass(Byte.class, new ByteSerializer(), KRYO_OFFSET + 23);
        serializer.registerClass(Short.class, new ShortSerializer(), KRYO_OFFSET + 24);
        serializer.registerClass(Character.class, new CharacterSerializer(), KRYO_OFFSET + 25);
        serializer.registerClass(Boolean.class, new BooleanSerializer(), KRYO_OFFSET + 26);
        serializer.registerClass(Object.class, KRYO_OFFSET + 27);
        serializer.registerClass(FullFloat.class, new FullFloatHandler(), KRYO_OFFSET + 28);
        serializer.registerClass(FullDouble.class, new FullDoubleHandler(), KRYO_OFFSET + 29);
        serializer.registerClass(char[].class, KRYO_OFFSET + 30);
        serializer.registerClass(short[].class, KRYO_OFFSET + 31);
        serializer.registerClass(float[].class, KRYO_OFFSET + 32);
        serializer.registerClass(Parameter.class,KRYO_OFFSET + 33);
        serializer.registerClass(Parameter[].class,KRYO_OFFSET + 34);
        serializer.registerClass(IndexParameters.class,KRYO_OFFSET + 35);
        serializer.registerClass(IndexParameters[].class,KRYO_OFFSET + 36);

        Preconditions.checkArgument(KRYO_OFFSET + 50 < RESERVED_ID_OFFSET, "ID allocation overflow!");
    }

}
