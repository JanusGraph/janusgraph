package com.thinkaurelius.titan.exceptions;



/**
 * Exception thrown by the middleware of the graph database.
 * 
 * Such exceptions are typically the result of atypical conditions during query processing and preparation and data storage preprocessing.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public class GraphDatabaseException extends RuntimeException {

	private static final long serialVersionUID = 4056436257763972423L;

	public GraphDatabaseException(String msg) {
		super(msg);
	}
	
	public GraphDatabaseException(String msg, Throwable e) {
		super(msg,e);
	}

	public GraphDatabaseException(Throwable e) {
		this("Exception in graph database.",e);
	}
	
	public boolean isCausedBy(Class<?> exType) {
		return ExceptionUtil.isCausedBy(this,exType);
	}
	


	
	
}
