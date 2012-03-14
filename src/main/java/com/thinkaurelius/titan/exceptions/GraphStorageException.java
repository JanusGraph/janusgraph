package com.thinkaurelius.titan.exceptions;

/**
 * Exception thrown by the storage backend of the graph database.
 * 
 * Such exceptions are typically caused by the underlying storage engine and re-thrown as {@link com.thinkaurelius.titan.exceptions.GraphStorageException}.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public class GraphStorageException extends RuntimeException {

	private static final long serialVersionUID = 4056436257763972423L;

	public GraphStorageException(String msg) {
		super(msg);
	}
	
	public GraphStorageException(String msg, Throwable e) {
		super(msg,e);
	}

	public GraphStorageException(Throwable e) {
		this("Exception in storage backend.",e);
	}
	
	public boolean isCausedBy(Class<?> exType) {
		return ExceptionUtil.isCausedBy(this,exType);
	}
	


	
	
}
