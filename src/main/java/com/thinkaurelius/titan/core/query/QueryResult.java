package com.thinkaurelius.titan.core.query;

public interface QueryResult<U> {

	public void add(U result);
	
	public void close();
	
}
