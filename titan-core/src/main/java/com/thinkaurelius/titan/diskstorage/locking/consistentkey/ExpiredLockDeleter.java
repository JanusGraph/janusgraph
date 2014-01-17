package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;

public class ExpiredLockDeleter implements Runnable {

    private final KeyColumnValueStore store;
    private final KeyColumn deletionTarget;
    private final TimestampProvider times;

    public ExpiredLockDeleter(KeyColumnValueStore store, KeyColumn deletionTarget, TimestampProvider times) {
        this.store = store;
        this.deletionTarget = deletionTarget;
        this.times = times;
    }

    @Override
    public void run() {

    }
}
