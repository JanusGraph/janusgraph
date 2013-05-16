package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class LongSerializer implements AttributeSerializer<Long> {

    private static final long serialVersionUID = -8438674418838450877L;

    @Override
    public Long read(ScanBuffer buffer) {
        return Long.valueOf(buffer.getLong() + Long.MIN_VALUE);
    }

    @Override
    public void writeObjectData(WriteBuffer out, Long object) {
        out.putLong(object.longValue() - Long.MIN_VALUE);
    }

}
