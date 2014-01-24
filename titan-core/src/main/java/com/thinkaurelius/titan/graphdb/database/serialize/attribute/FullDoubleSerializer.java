package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.core.attribute.FullDouble;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FullDoubleSerializer implements AttributeSerializer<FullDouble> {

    @Override
    public void verifyAttribute(FullDouble value) {
        //All values are valid
    }

    //Copied from DoubleSerializer
    @Override
    public FullDouble convert(Object value) {
        if (value instanceof Number) {
            return new FullDouble(((Number)value).doubleValue());
        } else if (value instanceof String) {
            return new FullDouble(Double.parseDouble((String)value));
        } else return null;
    }

    @Override
    public FullDouble read(ScanBuffer buffer) {
        return new FullDouble(buffer.getDouble());
    }

    @Override
    public void writeObjectData(WriteBuffer buffer, FullDouble attribute) {
        buffer.putDouble(attribute.doubleValue());
    }
}
