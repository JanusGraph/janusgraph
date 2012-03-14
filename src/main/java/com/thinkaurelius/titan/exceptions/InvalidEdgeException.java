package com.thinkaurelius.titan.exceptions;

/**
 * Exception thrown when an edge is invalid.
 * 
 * When an edge is considered invalid depends on the context, such as a duplicate edge added by a user.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public class InvalidEdgeException extends InvalidEntityException {

	private static final long serialVersionUID = 1L;

	public InvalidEdgeException(String msg) {
		super(msg);
	}

}
