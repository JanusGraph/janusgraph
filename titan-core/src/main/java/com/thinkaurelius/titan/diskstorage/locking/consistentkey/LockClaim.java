package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

import java.nio.ByteBuffer;

/**
 * An attempted lock.
 * <p/>
 * Contains the key, column, and expected-value at the key-column coordinate in
 * some underlying store. A reference to the store is also indirectly maintained
 * through a {@see LockConfig} instance passed to the constructor.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class LockClaim {

    private ByteBuffer cachedLockKey, lockCol;
    private long timestamp;

    private final ConsistentKeyLockStore backer;
    private final ByteBuffer expectedValue;
    private final KeyColumn kc;

    public LockClaim(ConsistentKeyLockStore backer, ByteBuffer key,
                     ByteBuffer column, ByteBuffer expectedValue) {

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

    public ByteBuffer getKey() {
        return kc.getKey();
    }

    public ByteBuffer getColumn() {
        return kc.getColumn();
    }

    public ByteBuffer getExpectedValue() {
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

    public ByteBuffer getLockKey() {
        if (null != cachedLockKey) {
            return cachedLockKey;
        }

        ByteBuffer key = kc.getKey();
        ByteBuffer column = kc.getColumn();

        cachedLockKey = ByteBuffer.allocate(key.remaining() + column.remaining() + 4);
        cachedLockKey.putInt(key.remaining()).put(key.duplicate()).put(column.duplicate()).rewind();

        return cachedLockKey;
    }

    public ByteBuffer getLockCol(long ts, byte[] rid) {

        lockCol = ByteBuffer.allocate(rid.length + 8);
        lockCol.putLong(ts).put(rid).rewind();

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
                + ", key=0x" + ByteBufferUtil.bytesToHex(kc.getKey())
                + ", col=0x" + ByteBufferUtil.bytesToHex(kc.getColumn())
                + ", expectedValue=" + (null == expectedValue ? "null" : "0x" + ByteBufferUtil.bytesToHex(expectedValue)) + "]";
    }
}