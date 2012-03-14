package com.thinkaurelius.titan.graphdb.database.util;

import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;

public class LimitTracker {

	private static final int retrievalLimit = 1000000000; //1 billion
	
	private int remainingLimit;
	private final boolean partialResult;
	
	public LimitTracker(InternalEdgeQuery query) {
		remainingLimit = (int)Math.min(retrievalLimit, query.getLimit());
		partialResult = query.returnPartialResult();
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
	
	public boolean partialResult() {
		return partialResult;
	}
	
}
