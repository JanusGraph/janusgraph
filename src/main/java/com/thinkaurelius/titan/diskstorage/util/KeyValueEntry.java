package com.thinkaurelius.titan.diskstorage.util;

import java.nio.ByteBuffer;

public class KeyValueEntry {

	private final ByteBuffer key;
	private final ByteBuffer value;
	
	public KeyValueEntry(ByteBuffer key, ByteBuffer value) {
		assert key!=null;
		assert value!=null;
		this.key=key;
		this.value=value;
	}
	
	
	public ByteBuffer getKey() {
		return key;
	}


	public ByteBuffer getValue() {
		return value;
	}
	
	
	
}
