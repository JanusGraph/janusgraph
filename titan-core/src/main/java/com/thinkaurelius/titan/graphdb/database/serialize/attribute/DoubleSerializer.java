package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoubleSerializer implements AttributeSerializer<Double> {

    private static final Logger log =
            LoggerFactory.getLogger(DoubleSerializer.class);

    private static final long serialVersionUID = -1719511496523862718L;

    public static final double EPSILON = 0.00000001;
    public static final int DECIMALS = 6;
    private static long MULTIPLIER = 0;

    static {
        MULTIPLIER = 1;
        for (int i = 0; i < DECIMALS; i++) MULTIPLIER *= 10;
    }

    public static final double MIN_VALUE = Long.MIN_VALUE*1.0 / (MULTIPLIER+1);
    public static final double MAX_VALUE = Long.MAX_VALUE*1.0 / (MULTIPLIER+1);

    private final LongSerializer ls = new LongSerializer();


    @Override
    public Double read(ScanBuffer buffer) {
        long longvalue = ls.read(buffer);
        return Double.valueOf(convert(longvalue));
    }

    @Override
    public void writeObjectData(WriteBuffer out, Double object) {
        Preconditions.checkArgument(withinRange(object),"Double value is out of range: %s",object);
        assert object.doubleValue() * MULTIPLIER>=Long.MIN_VALUE && object.doubleValue() * MULTIPLIER<=Long.MAX_VALUE;
        long convert = convert(object);
        ls.writeObjectData(out,convert);
    }

    public static final long convert(Double object) {
        return Math.round(object.doubleValue() * MULTIPLIER);
    }

    public static final double convert(long value) {
        return ((double) value) / MULTIPLIER;
    }

    public static final boolean withinRange(Double object) {
        return object.doubleValue()>=MIN_VALUE && object.doubleValue()<=MAX_VALUE;
    }

    /*
    ========= Float and Double implementations are similar
     */

    @Override
    public void verifyAttribute(Double value) {
        Preconditions.checkArgument(!Double.isNaN(value),"Value may not be NaN");
        Preconditions.checkArgument(withinRange(value),"Value out of range [%s,%s]: %s. Use FullDouble instead.",MIN_VALUE,MAX_VALUE,value);
        if (Math.abs(convert(convert(value))-value)>EPSILON) log.warn("Truncated decimals of double value: {}. Use FullDouble for full precision.",value);
    }

    @Override
    public Double convert(Object value) {
        if (value instanceof Number) {
            return ((Number)value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String)value);
        } else return null;
    }
}
