package org.janusgraph.diskstorage.locking.consistentkey;

import org.janusgraph.diskstorage.StaticBuffer;

import java.time.Instant;

public class TimestampRid {
    
    private final Instant timestamp;
    private final StaticBuffer rid;
    
    public TimestampRid(Instant timestamp, StaticBuffer rid) {
        this.timestamp = timestamp;
        this.rid = rid;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public StaticBuffer getRid() {
        return rid;
    }
}
