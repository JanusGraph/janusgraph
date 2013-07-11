package com.thinkaurelius.titan.diskstorage.cassandra.astyanax.locking;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.thinkaurelius.titan.diskstorage.locking.LockStatus;

public class AstyanaxLockStatus implements LockStatus {
    
    private final long time;
    private final TimeUnit timeUnit;
    private final ColumnPrefixDistributedRowLock<ByteBuffer> lock;
    
    public AstyanaxLockStatus(long time, TimeUnit timeUnit,
            ColumnPrefixDistributedRowLock<ByteBuffer> lock) {
        this.time = time;
        this.timeUnit = timeUnit;
        this.lock = lock;
    }
    
    @Override
    public long getExpirationTimestamp(TimeUnit tu) {
        return tu.convert(time, timeUnit);
    }

    public ColumnPrefixDistributedRowLock<ByteBuffer> getLock() {
        return lock;
    }
}