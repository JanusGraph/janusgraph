package com.thinkaurelius.titan.diskstorage.cassandra.astyanax.locking;

import java.nio.ByteBuffer;

import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.thinkaurelius.titan.core.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.locking.LockStatus;

public class AstyanaxLockStatus implements LockStatus {

    private final Timepoint expiration;
    private final ColumnPrefixDistributedRowLock<ByteBuffer> lock;

    public AstyanaxLockStatus(Timepoint expiration,
            ColumnPrefixDistributedRowLock<ByteBuffer> lock) {
        this.expiration = expiration;
        this.lock = lock;
    }

    @Override
    public Timepoint getExpirationTimestamp() {
        return expiration;
    }

    public ColumnPrefixDistributedRowLock<ByteBuffer> getLock() {
        return lock;
    }
}