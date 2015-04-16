package com.thinkaurelius.titan.graphdb.serializer.attributes;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

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
