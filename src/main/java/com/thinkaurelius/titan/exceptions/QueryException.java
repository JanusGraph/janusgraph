package com.thinkaurelius.titan.exceptions;

/**
 * Exception thrown when a user defined query is invalid.
 * 
 * A query is invalid if it violates some assumption about the query format or contains unknown entities.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public class QueryException extends GraphDatabaseException {

	private static final long serialVersionUID = 1L;
	
	public QueryException(String msg) {
		super(msg);
	}
	
	public QueryException(String msg, Throwable e) {
		super(msg,e);
	}

	public QueryException(Throwable e) {
		this("Exception in query.",e);
	}
	
	public boolean isCausedBy(Class<?> exType) {
		return ExceptionUtil.isCausedBy(this,exType);
	}

}
