package com.thinkaurelius.titan.core;


import com.thinkaurelius.titan.util.datastructures.ExceptionUtil;

/**
 * Most general type of exception thrown by the Titan graph database.
 *
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * 
 * 
 */
public class GraphDatabaseException extends RuntimeException {

	private static final long serialVersionUID = 4056436257763972423L;

    /**
     *
     * @param msg Exception message
     */
	public GraphDatabaseException(String msg) {
		super(msg);
	}

    /**
     *
     * @param msg Exception message
     * @param cause Cause of the exception
     */
	public GraphDatabaseException(String msg, Throwable cause) {
		super(msg,cause);
	}

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
	public GraphDatabaseException(Throwable cause) {
		this("Exception in graph database.",cause);
	}

    /**
     * Checks whether this exception is cause by an exception of the given type.
     * @param causeExceptionType exception type
     * @return true, if this exception is caused by the given type
     */
	public boolean isCausedBy(Class<?> causeExceptionType) {
		return ExceptionUtil.isCausedBy(this, causeExceptionType);
	}
	
    public static final RuntimeException transactionNotOpenException() {
        return new IllegalStateException("Operation cannot be executed because the enclosing transaction is closed");
    }

	
	
}
