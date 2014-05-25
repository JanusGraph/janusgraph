package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class DoubleSerializer implements AttributeSerializer<Double> {

    @Override
    public void verifyAttribute(Double value) {
        //All values are valid
    }

    @Override
    public Double convert(Object value) {
        if (value instanceof Number) {
            return Double.valueOf(((Number) value).doubleValue());
        } else if (value instanceof String) {
            return Double.valueOf(Double.parseDouble((String) value));
        } else return null;
    }

    @Override
    public Double read(ScanBuffer buffer) {
        return buffer.getDouble();
    }

    @Override
    public void write(WriteBuffer buffer, Double attribute) {
        buffer.putDouble(attribute.doubleValue());
    }
}
