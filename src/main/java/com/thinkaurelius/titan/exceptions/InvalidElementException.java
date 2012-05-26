package com.thinkaurelius.titan.exceptions;

import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanVertex;

/**
 * Exception thrown when a entity is invalid.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public class InvalidElementException extends GraphDatabaseException {

    private final TitanVertex element;
    
	public InvalidElementException(String msg, TitanVertex element) {
		super(msg);
        this.element = element;
	}
	
}
