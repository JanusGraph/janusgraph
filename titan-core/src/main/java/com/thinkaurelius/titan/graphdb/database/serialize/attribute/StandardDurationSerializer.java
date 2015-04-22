package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;

import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardDurationSerializer implements AttributeSerializer<StandardDuration> {

    private final LongSerializer longs = new LongSerializer();
    private final EnumSerializer<TimeUnit> units = new EnumSerializer<>(TimeUnit.class);

    @Override
    public StandardDuration read(ScanBuffer buffer) {
        TimeUnit unit = units.read(buffer);
        long length = longs.read(buffer);
        return new StandardDuration(length,unit);
    }

    @Override
    public void write(WriteBuffer buffer, StandardDuration attribute) {
        TimeUnit nativeUnit = attribute.getNativeUnit();
        units.write(buffer,nativeUnit);
        longs.write(buffer,attribute.getLength(nativeUnit));
    }
}
