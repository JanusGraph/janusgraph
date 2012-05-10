package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;

import java.nio.ByteBuffer;

public class FloatSerializer implements AttributeSerializer<Float> {

	private static final long serialVersionUID = -1719511423423862718L;


    public static final int DECIMALS = 3;
    private static int MULTIPLIER = 0;

    static {
        MULTIPLIER =1;
        for (int i=0;i<DECIMALS;i++) MULTIPLIER *=10;
    }

    @Override
    public Float read(ByteBuffer buffer) {
        long convert = buffer.getLong();
        convert = convert + Long.MIN_VALUE;
        return Float.valueOf(((float)convert)/ MULTIPLIER);
    }

    @Override
    public void writeObjectData(ByteBuffer buffer, Float object) {
        long convert = (long)(object.doubleValue()*MULTIPLIER) - Long.MIN_VALUE;
        buffer.putLong(convert);
    }

}
