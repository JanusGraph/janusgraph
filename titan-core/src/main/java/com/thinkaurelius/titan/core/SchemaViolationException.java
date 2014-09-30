package com.thinkaurelius.titan.core;

/**
 * Titan represents element identifiers as longs, but not all numbers
 * in the representable space of longs are valid.  This exception can
 * be thrown when an invalid long ID is encountered.
 */
public class SchemaViolationException extends TitanException {

    public SchemaViolationException(String msg) {
        super(msg);
    }

    public SchemaViolationException(String msg, Object... args) {
        super(String.format(msg,args));
    }

}
