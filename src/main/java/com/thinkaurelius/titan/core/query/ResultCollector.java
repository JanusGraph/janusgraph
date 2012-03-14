package com.thinkaurelius.titan.core.query;

public interface ResultCollector<T> {

	
	
	public Class<T> resultType();	
	
	public void added(Object result);
	
	public void close();
	
	public void abort();
	
}
