package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.core.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class TimepointSerializer implements AttributeSerializer<Timepoint> {

    @Override
    public void verifyAttribute(Timepoint value) { }

    @Override
    public Timepoint convert(Object value) {
        throw new IllegalArgumentException();
    }

    @Override
    public Timepoint read(ScanBuffer buffer) {
        return new Timepoint(buffer.getLong(), TimeUnit.values()[buffer.getByte()]);
    }

    @Override
    public void writeObjectData(WriteBuffer buffer, Timepoint attribute) {
        TimeUnit u = attribute.getNativeUnit();
        buffer.putLong(attribute.getTime(u));
        buffer.putByte((byte)u.ordinal());
    }

}
