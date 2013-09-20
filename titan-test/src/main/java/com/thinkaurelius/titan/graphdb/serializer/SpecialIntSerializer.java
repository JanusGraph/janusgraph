package com.thinkaurelius.titan.graphdb.serializer;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SpecialIntSerializer implements AttributeSerializer<SpecialInt> {

    @Override
    public SpecialInt read(ScanBuffer buffer) {
        return new SpecialInt(buffer.getInt());
    }

    @Override
    public void writeObjectData(WriteBuffer out, SpecialInt attribute) {
        out.putInt(attribute.getValue());
    }

    @Override
    public void verifyAttribute(SpecialInt value) {
        //All value are valid;
    }

    @Override
    public SpecialInt convert(Object value) {
        return null;
    }
}