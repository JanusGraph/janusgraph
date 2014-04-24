package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.core.time.SimpleDuration;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class SimpleDurationSerializer implements AttributeSerializer<SimpleDuration> {

    @Override
    public void verifyAttribute(SimpleDuration value) { }

    @Override
    public SimpleDuration convert(Object value) {
        throw new IllegalArgumentException();
    }

    @Override
    public SimpleDuration read(ScanBuffer buffer) {
        return new SimpleDuration(buffer.getLong(), TimeUnit.values()[buffer.getByte()]);
    }

    @Override
    public void writeObjectData(WriteBuffer buffer, SimpleDuration attribute) {
        TimeUnit u = attribute.getNativeUnit();
        buffer.putLong(attribute.getLength(u));
        buffer.putByte((byte)u.ordinal());
    }
}
