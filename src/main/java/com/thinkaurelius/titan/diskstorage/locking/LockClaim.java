package com.thinkaurelius.titan.diskstorage.locking;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

public class LockClaim {
	private final String cf;
	private final ByteBuffer key;
	private final ByteBuffer column;
	private final ByteBuffer expectedValue;
	private final KeyColumn kc;
	private ByteBuffer lockKey, lockCol;
	private long timestamp;
	
	public LockClaim(String cf, ByteBuffer key, ByteBuffer column,
			ByteBuffer expectedValue) {
		this.cf = cf;
		this.key = key;
		this.column = column;
		this.expectedValue = expectedValue;
		this.kc = new KeyColumn(this.key, this.column);
		
		assert null != this.cf;
		assert null != this.key;
		assert null != this.column;
	}
	
	public String getCf() {
		return cf;
	}
	
	public ByteBuffer getKey() {
		return key;
	}
	
	public ByteBuffer getColumn() {
		return column;
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
		if (null != lockKey) {
			return lockKey;
		}
		
		lockKey = ByteBuffer.allocate(key.remaining() + column.remaining() + 4);
		lockKey.putInt(key.remaining()).put(key.duplicate()).put(column.duplicate()).reset();
		
		return lockKey;
	}
	
	public ByteBuffer getLockCol(long ts, byte[] rid) {
		
		lockCol = ByteBuffer.allocate(rid.length + 8);
		lockCol.putLong(ts).put(rid).reset();
		
		return lockCol;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + cf.hashCode();
		result = prime * result + column.hashCode();
		result = prime * result + key.hashCode();
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
		return other.cf.equals(this.cf) &&
				other.key.equals(this.key) &&
				other.column.equals(this.column);
	}

	@Override
	public String toString() {
		return "LockClaim [cf=" + cf 
				+ ", key=0x" + ByteBufferUtil.bytesToHex(key) 
			    + ", column=0x" + ByteBufferUtil.bytesToHex(column)
				+ ", expectedValue=" + (null == expectedValue ? "null" : "0x" + ByteBufferUtil.bytesToHex(expectedValue)) + "]";
	}
	
	
}