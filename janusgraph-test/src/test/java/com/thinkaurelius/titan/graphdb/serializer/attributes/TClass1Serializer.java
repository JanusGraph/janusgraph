package com.thinkaurelius.titan.graphdb.serializer.attributes;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TClass1Serializer implements AttributeSerializer<TClass1> {

    @Override
    public TClass1 read(ScanBuffer buffer) {
        return new TClass1(buffer.getLong(),buffer.getFloat());
    }

    @Override
    public void write(WriteBuffer buffer, TClass1 attribute) {
        buffer.putLong(attribute.getA());
        buffer.putFloat(attribute.getF());
    }

}
