package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.core.GraphStorageException;

/**
 * This exception signifies a either a technical failure during locking, such as
 * failure in the connection to the underyling storage system, or an attempt to
 * acquire a lock already held by another transaction.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class LockingFailureException extends GraphStorageException {

	private static final long serialVersionUID = 6664344125217556566L;

	public LockingFailureException(String msg) {
		super(msg);
	}
	
	public LockingFailureException(String msg, Throwable e) {
		super(msg,e);
	}

	public LockingFailureException(Throwable e) {
		this("Locking failure", e);
	}
}
