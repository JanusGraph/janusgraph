package com.thinkaurelius.titan.exceptions;

/**
 * Exception thrown when a entity is invalid.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public class InvalidEntityException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public InvalidEntityException(String msg) {
		super(msg);
	}
	
}
