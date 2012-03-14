package com.thinkaurelius.titan.exceptions;

/**
 * Exception thrown when a not yet implemented piece of code is invoked.
 * 
 * This graph database is currently under development and some functionality may not yet be available.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public class ToBeImplementedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ToBeImplementedException(String msg) {
		super(msg);
	}
	
	public ToBeImplementedException() {
		super("This functionality has not yet been implemented!");
	}
	
}
