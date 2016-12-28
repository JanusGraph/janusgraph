package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

import java.time.Duration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class DurationSerializer implements AttributeSerializer<Duration> {

    private final LongSerializer secondsSerializer = new LongSerializer();
    private final IntegerSerializer nanosSerializer = new IntegerSerializer();

    @Override
    public Duration read(ScanBuffer buffer) {
        long seconds = secondsSerializer.read(buffer);
        long nanos = nanosSerializer.read(buffer);
        return Duration.ofSeconds(seconds, nanos);
    }

    @Override
    public void write(WriteBuffer buffer, Duration attribute) {
        secondsSerializer.write(buffer,attribute.getSeconds());
        nanosSerializer.write(buffer,attribute.getNano());
    }
}
