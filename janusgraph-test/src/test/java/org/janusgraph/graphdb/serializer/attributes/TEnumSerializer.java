package org.janusgraph.graphdb.serializer.attributes;

import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TEnumSerializer implements AttributeSerializer<TEnum> {

    @Override
    public TEnum read(ScanBuffer buffer) {
        return TEnum.values()[buffer.getShort()];
    }

    @Override
    public void write(WriteBuffer buffer, TEnum attribute) {
        buffer.putShort((short)attribute.ordinal());
    }
}
