package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.core.attribute.FullFloat;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FullFloatSerializer implements AttributeSerializer<FullFloat> {

    @Override
    public void verifyAttribute(FullFloat value) {
        //All values are valid
    }

    //Copied from FloatSerializer
    @Override
    public FullFloat convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (d<Float.MIN_VALUE || d>Float.MAX_VALUE) throw new IllegalArgumentException("Value too large for float: " + value);
            return new FullFloat((float)d);
        } else if (value instanceof String) {
            return new FullFloat(Float.parseFloat((String) value));
        } else return null;
    }

    @Override
    public FullFloat read(ScanBuffer buffer) {
        return new FullFloat(buffer.getFloat());
    }

    @Override
    public void writeObjectData(WriteBuffer buffer, FullFloat attribute) {
        buffer.putFloat(attribute.floatValue());
    }
}
