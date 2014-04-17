package com.thinkaurelius.titan.diskstorage.cassandra.astyanax.locking;

import java.nio.ByteBuffer;

import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.thinkaurelius.titan.diskstorage.locking.LockStatus;

public class AstyanaxLockStatus implements LockStatus {
    
    private final long time;
    private final ColumnPrefixDistributedRowLock<ByteBuffer> lock;
    
    public AstyanaxLockStatus(long time,
            ColumnPrefixDistributedRowLock<ByteBuffer> lock) {
        this.time = time;
        this.lock = lock;
    }
    
    @Override
    public long getExpirationTimestamp() {
        return time;
    }

    public ColumnPrefixDistributedRowLock<ByteBuffer> getLock() {
        return lock;
    }
}