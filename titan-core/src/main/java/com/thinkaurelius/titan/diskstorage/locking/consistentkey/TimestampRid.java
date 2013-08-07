package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;

public class TimestampRid {
    
    private final long timestamp;
    private final StaticBuffer rid;
    
    public TimestampRid(long timestamp, StaticBuffer rid) {
        this.timestamp = timestamp;
        this.rid = rid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public StaticBuffer getRid() {
        return rid;
    }
}
