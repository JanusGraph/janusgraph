package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public class LimitedSelector implements KeySelector {

	private final int limit;
	private int count;
	
	public LimitedSelector(int limit) {
		Preconditions.checkArgument(limit>0,"The count limit needs to be positive. Given: " + limit);
		this.limit=limit;
		count=0;
	}
	
	public static final LimitedSelector of(int limit) {
		return new LimitedSelector(limit);
	}

	@Override
	public boolean include(ByteBuffer key) {
		count++;
		return true;
	}

	@Override
	public boolean reachedLimit() {
		if (count>=limit) return true;
		else return false;
	}

}
