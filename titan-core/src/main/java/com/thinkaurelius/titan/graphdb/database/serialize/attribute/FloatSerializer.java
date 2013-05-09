package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class FloatSerializer implements AttributeSerializer<Float> {

    private static final long serialVersionUID = -1719511423423862718L;


    public static final int DECIMALS = 3;
    private static int MULTIPLIER = 0;

    static {
        MULTIPLIER = 1;
        for (int i = 0; i < DECIMALS; i++) MULTIPLIER *= 10;
    }

    public static final float MIN_VALUE = (float)(Long.MIN_VALUE*1.0 / (MULTIPLIER+1));
    public static final float MAX_VALUE = (float)(Long.MAX_VALUE*1.0 / (MULTIPLIER+1));

    private final LongSerializer ls = new LongSerializer();

    @Override
    public Float read(ScanBuffer buffer) {
        long convert = ls.read(buffer);
        return Float.valueOf(((float) convert) / MULTIPLIER);
    }

    @Override
    public void writeObjectData(WriteBuffer out, Float object) {
        Preconditions.checkArgument(withinRange(object), "Float value is out of range: %s", object);
        assert object.floatValue() * MULTIPLIER>=Long.MIN_VALUE && object.floatValue() * MULTIPLIER<=Long.MAX_VALUE;
        long convert = (long) (object.floatValue() * MULTIPLIER);
        ls.writeObjectData(out,convert);
    }

    public static final boolean withinRange(Float object) {
        return object.floatValue()>=MIN_VALUE && object.floatValue()<=MAX_VALUE;
    }

}
