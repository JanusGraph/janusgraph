package com.thinkaurelius.titan.diskstorage.locking;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

public class KeyColumn {
	
	private final ByteBuffer key;
	private final ByteBuffer col;
	
	public KeyColumn(ByteBuffer key, ByteBuffer col) {
		this.key = key;
		this.col = col;
		
		assert null != this.key;
		assert null != this.col;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + col.hashCode();
		result = prime * result + key.hashCode();
        //TODO: if the hashcode is needed frequently, we should store it
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyColumn other = (KeyColumn) obj;
		return other.key.equals(key) && other.col.equals(col);
	}

	@Override
	public String toString() {
		return "KeyColumn [k=0x" + ByteBufferUtil.bytesToHex(key) + 
				", c=0x" + ByteBufferUtil.bytesToHex(col) + "]";
	}
}