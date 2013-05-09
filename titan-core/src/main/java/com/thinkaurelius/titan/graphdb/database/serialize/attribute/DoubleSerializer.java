package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class DoubleSerializer implements AttributeSerializer<Double> {

    private static final long serialVersionUID = -1719511496523862718L;

    public static final int DECIMALS = 6;
    private static int MULTIPLIER = 0;

    static {
        MULTIPLIER = 1;
        for (int i = 0; i < DECIMALS; i++) MULTIPLIER *= 10;
    }

    public static final double MIN_VALUE = Long.MIN_VALUE*1.0 / (MULTIPLIER+1);
    public static final double MAX_VALUE = Long.MAX_VALUE*1.0 / (MULTIPLIER+1);

    private final LongSerializer ls = new LongSerializer();


    @Override
    public Double read(ScanBuffer buffer) {
        long convert = ls.read(buffer);
        return Double.valueOf(((double) convert) / MULTIPLIER);
    }

    @Override
    public void writeObjectData(WriteBuffer out, Double object) {
        Preconditions.checkArgument(withinRange(object),"Double value is out of range: %s",object);
        assert object.doubleValue() * MULTIPLIER>=Long.MIN_VALUE && object.doubleValue() * MULTIPLIER<=Long.MAX_VALUE;
        long convert = (long) (object.doubleValue() * MULTIPLIER);
        ls.writeObjectData(out,convert);
    }

    public static final boolean withinRange(Double object) {
        return object.doubleValue()>=MIN_VALUE && object.doubleValue()<=MAX_VALUE;
    }

}
