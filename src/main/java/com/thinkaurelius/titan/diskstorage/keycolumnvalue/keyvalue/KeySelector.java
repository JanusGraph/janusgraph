package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import java.nio.ByteBuffer;

public interface KeySelector {
	
	public static final KeySelector SelectAll = new KeySelector() {

		@Override
		public boolean include(ByteBuffer key) {
			return true;
		}
		
		@Override
		public boolean reachedLimit() {
			return false;
		}
		
	};

	public boolean include(ByteBuffer key);
	
	public boolean reachedLimit();
	
}
