package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

import java.time.Instant;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class InstantSerializer implements AttributeSerializer<Instant> {

    private final LongSerializer secondsSerializer = new LongSerializer();
    private final IntegerSerializer nanosSerializer = new IntegerSerializer();

    @Override
    public Instant read(ScanBuffer buffer) {
        long seconds = secondsSerializer.read(buffer);
        long nanos = nanosSerializer.read(buffer);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    @Override
    public void write(WriteBuffer buffer, Instant attribute) {
        secondsSerializer.write(buffer, attribute.getEpochSecond());
        nanosSerializer.write(buffer,attribute.getNano());
    }
}
