package com.thinkaurelius.titan.exceptions;

/**
 * Exception thrown when a node is invalid.
 * 
 * When a node is considered invalid depends on the context, such as handing a node to a transaction of which it is not actually a part.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public class InvalidNodeException extends InvalidEntityException {

	private static final long serialVersionUID = 1L;

	public InvalidNodeException(String msg) {
		super(msg);
	}

}
