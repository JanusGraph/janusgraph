package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.util.datastructures.ExceptionUtil;

/**
 * Exception thrown in the storage layer of the graph database.
 * 
 * Such exceptions are typically caused by the underlying storage engine and re-thrown as {@link GraphStorageException}.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 */
public class GraphStorageException extends GraphDatabaseException {

	private static final long serialVersionUID = 4056436257763972423L;

    /**
     *
     * @param msg Exception message
     */
	public GraphStorageException(String msg) {
		super(msg);
	}

    /**
     *
     * @param msg Exception message
     * @param cause Cause of the exception
     */
	public GraphStorageException(String msg, Throwable cause) {
		super(msg,cause);
	}

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
	public GraphStorageException(Throwable cause) {
		this("Exception in storage backend.",cause);
	}

	


	
	
}
