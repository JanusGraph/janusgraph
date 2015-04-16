package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.core.attribute.Timestamp;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;

import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TimestampSerializer implements AttributeSerializer<Timestamp> {

    private final LongSerializer longs = new LongSerializer();
    private final EnumSerializer<TimeUnit> units = new EnumSerializer<>(TimeUnit.class);

    @Override
    public Timestamp read(ScanBuffer buffer) {
        TimeUnit unit = units.read(buffer);
        long length = longs.read(buffer);
        return new Timestamp(length,unit);
    }

    @Override
    public void write(WriteBuffer buffer, Timestamp attribute) {
        TimeUnit nativeUnit = attribute.getNativeUnit();
        units.write(buffer,nativeUnit);
        longs.write(buffer,attribute.sinceEpoch(nativeUnit));
    }
}
