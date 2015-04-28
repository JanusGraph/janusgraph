package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardTimepoint;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;

import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTimepointSerializer implements AttributeSerializer<Timepoint> {

    private final LongSerializer longs = new LongSerializer();
    private final EnumSerializer<Timestamps> timestamp = new EnumSerializer<>(Timestamps.class);

    @Override
    public StandardTimepoint read(ScanBuffer buffer) {
        Timestamps ts = timestamp.read(buffer);
        long length = longs.read(buffer);
        return new StandardTimepoint(length,ts);
    }

    @Override
    public void write(WriteBuffer buffer, Timepoint attribute) {
        TimestampProvider provider = attribute.getProvider();
        Preconditions.checkArgument(provider instanceof Timestamps,"Cannot serialize time point due to invalid provider: %s",attribute);
        timestamp.write(buffer, (Timestamps)provider);
        longs.write(buffer,attribute.getNativeTimestamp());
    }
}
