package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.WriteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;

/**
 * An attempted lock.
 * <p/>
 * Contains the key, column, and expected-value at the key-column coordinate in
 * some underlying store. A reference to the store is also indirectly maintained
 * through a {@see LockConfig} instance passed to the constructor.
 *
 * @see ConsistentKeyLockTransaction
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class LockClaim {

    private StaticBuffer cachedLockKey, lockCol;
    private long timestamp;

    private final ConsistentKeyLockStore backer;
    private final StaticBuffer expectedValue;
    private final KeyColumn kc;

    public LockClaim(ConsistentKeyLockStore backer, StaticBuffer key,
                     StaticBuffer column, StaticBuffer expectedValue) {

        assert null != backer;
        assert null != key;
        assert null != column;

        this.backer = backer;
        this.expectedValue = expectedValue;
        this.kc = new KeyColumn(key, column);

    }

    public ConsistentKeyLockStore getBacker() {
        return backer;
    }

    public StaticBuffer getKey() {
        return kc.getKey();
    }

    public StaticBuffer getColumn() {
        return kc.getColumn();
    }

    public StaticBuffer getExpectedValue() {
        return expectedValue;
    }

    public KeyColumn getKc() {
        return kc;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public StaticBuffer getLockKey() {
        if (null != cachedLockKey) return cachedLockKey;

        StaticBuffer key = kc.getKey();
        StaticBuffer column = kc.getColumn();

        WriteBuffer b = new WriteByteBuffer(key.length() + column.length() + 4);
        b.putInt(key.length());
        WriteBufferUtil.put(b,key);
        WriteBufferUtil.put(b,column);
        cachedLockKey = b.getStaticBuffer();
        return cachedLockKey;
    }

    public StaticBuffer getLockCol(long ts, byte[] rid) {
        /*
         * ConsistentKeyLockTransaction#writeBlindLockClaim(...) calls this
         * method with a current timestamp and writes the result to storage. If
         * said write takes longer than the configured lock wait time, then
         * writeBlindLockClaim(...) deletes the column, generates another
         * current timestamp, calls this method again with the updated
         * timestamp, and writes the resulting column. If this method returns a
         * stale cached column containing the old timestamp, then lock
         * verification will later fail with a "timestamp mismatch" exception
         * 
         * Either writeBlindLockClaim(...) must be modified to use the same
         * timestamp for all attempts to write any given lock (even if some
         * writes take too long and have to be retried), or this method must not
         * cache the return value irrespective of the ts argument.
         * 
         * I'm taking the latter approach here for the stable branch because
         * it's a one-liner change.
         */
        //if (null != lockCol) return lockCol;

        WriteBuffer b = new WriteByteBuffer(rid.length + 8);
        b.putLong(ts);
        WriteBufferUtil.put(b,rid);
        lockCol = b.getStaticBuffer();
        return lockCol;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + backer.hashCode();
        result = prime * result + kc.hashCode();
        return result;
    }

    /**
     * Equals only examines the column family, key, and column.
     * <p/>
     * expectedValue is deliberately ignored.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LockClaim other = (LockClaim) obj;
        return other.kc.equals(this.kc) &&
                other.backer.equals(this.backer);
    }

    @Override
    public String toString() {
        return "LockClaim [backer=" + backer
                + ", key=0x" + kc.getKey()
                + ", col=0x" + kc.getColumn()
                + ", expectedValue=" + (null == expectedValue ? "null" : "0x" + expectedValue) + "]";
    }
}