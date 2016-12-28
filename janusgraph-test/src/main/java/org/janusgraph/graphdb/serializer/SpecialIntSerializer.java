package org.janusgraph.graphdb.serializer;

import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SpecialIntSerializer implements AttributeSerializer<SpecialInt> {

    @Override
    public SpecialInt read(ScanBuffer buffer) {
        return new SpecialInt(buffer.getInt());
    }

    @Override
    public void write(WriteBuffer out, SpecialInt attribute) {
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
