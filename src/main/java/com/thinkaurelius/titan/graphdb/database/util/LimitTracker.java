package com.thinkaurelius.titan.graphdb.database.util;

import com.thinkaurelius.titan.graphdb.query.InternalTitanQuery;

public class LimitTracker {

	private static final int retrievalLimit = 2000000000; //2 billion
	
	private int remainingLimit;

	public LimitTracker(InternalTitanQuery query) {
		remainingLimit = (int)Math.min(retrievalLimit, query.getLimit());
	}
	
	public boolean limitExhausted() {
		return remainingLimit<=0;
	}
	
	public int getLimit() {
		return remainingLimit;
	}
	
	public void retrieved(int count) {
		remainingLimit -= count;
	}

}
