package com.thinkaurelius.titan.diskstorage.locking;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.thinkaurelius.titan.diskstorage.LockConfig;

public class LockClaim {

	private ByteBuffer cachedLockKey, lockCol;
	private long timestamp;
	
	private final LockConfig backer;
	private final ByteBuffer expectedValue;
	private final KeyColumn kc;
	
	public LockClaim(LockConfig backer, ByteBuffer key,
			ByteBuffer column, ByteBuffer expectedValue) {

		assert null != backer;
		assert null != key;
		assert null != column;
		
		this.backer = backer;
		this.expectedValue = expectedValue;
		this.kc = new KeyColumn(key, column);
		
	}
	
	public LockConfig getBacker() {
		return backer;
	}

	public ByteBuffer getKey() {
		return kc.getKey();
	}
	
	public ByteBuffer getColumn() {
		return kc.getCol();
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
		ByteBuffer column = kc.getCol();
		
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
	 * 
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
			    + ", col=0x" + ByteBufferUtil.bytesToHex(kc.getCol())
				+ ", expectedValue=" + (null == expectedValue ? "null" : "0x" + ByteBufferUtil.bytesToHex(expectedValue)) + "]";
	}
	
	
}