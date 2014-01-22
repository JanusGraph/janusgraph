package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FloatSerializer implements AttributeSerializer<Float> {

    private static final Logger log =
            LoggerFactory.getLogger(FloatSerializer.class);

    private static final long serialVersionUID = -1719511423423862718L;


    public static final int DECIMALS = 3;
    private static long MULTIPLIER = 0;

    static {
        MULTIPLIER = 1;
        for (int i = 0; i < DECIMALS; i++) MULTIPLIER *= 10;
    }

    public static final float MIN_VALUE = (float)(Long.MIN_VALUE*1.0 / (MULTIPLIER+1));
    public static final float MAX_VALUE = (float)(Long.MAX_VALUE*1.0 / (MULTIPLIER+1));

    private final LongSerializer ls = new LongSerializer();

    @Override
    public Float read(ScanBuffer buffer) {
        long longvalue = ls.read(buffer);
        return Float.valueOf(convert(longvalue));
    }

    @Override
    public void writeObjectData(WriteBuffer out, Float object) {
        Preconditions.checkArgument(withinRange(object), "Float value is out of range: %s", object);
        assert object.floatValue() * MULTIPLIER>=Long.MIN_VALUE && object.floatValue() * MULTIPLIER<=Long.MAX_VALUE;
        long convert = convert(object);
        ls.writeObjectData(out,convert);
    }

    public static final long convert(Float object) {
        return Math.round(object.doubleValue() * MULTIPLIER);
    }

    public static final float convert(long value) {
        return ((float) value) / MULTIPLIER;
    }

    public static final boolean withinRange(Float object) {
        return object.floatValue()>=MIN_VALUE && object.floatValue()<=MAX_VALUE;
    }

        /*
    ========= Float and Double implementations are similar
     */

    @Override
    public void verifyAttribute(Float value) {
        Preconditions.checkArgument(!Float.isNaN(value),"Value may not be NaN");
        Preconditions.checkArgument(withinRange(value),"Value out of range [%s,%s]: %s. Use FullFloat instead.",MIN_VALUE,MAX_VALUE,value);
        if (Math.abs(convert(convert(value))-value)>DoubleSerializer.EPSILON) log.warn("Truncated decimals of float value: {}. Use FullFloat for full precision.",value);
    }

    @Override
    public Float convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (d<Float.MIN_VALUE || d>Float.MAX_VALUE) throw new IllegalArgumentException("Value too large for float: " + value);
            return Float.valueOf((float)d);
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        } else return null;
    }

}

