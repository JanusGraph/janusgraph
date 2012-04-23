package com.thinkaurelius.titan.diskstorage;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface LockKeyColumnValueStore {

    /**
     * Acquires a lock for the key-column pair which ensures that nobody else can take a lock on that
     * respective entry for the duration of this lock (but somebody could potentially still overwrite
     * the key-value entry without taking a lock).
     * The expectedValue defines the value expected to match the value at the time the lock is acquired (or null if it is expected
     * that the key-column pair does not exist).
     *
     * If this method is called multiple times with the same key-column pair in the same transaction, all but the first invocation are ignored.
     *
     * The lock has to be released when the transaction closes (commits or aborts).
     *
     * @param key Key on which to lock
     * @param column Column the column on which to lock
     * @param expectedValue The expected value for the specified key-column pair on which to lock. Null if it is expected that the pair does not exist
     * @param txh Transaction
     */
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, TransactionHandle txh);

}
