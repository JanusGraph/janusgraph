package org.janusgraph.core;

/**
 * Janus represents element identifiers as longs, but not all numbers
 * in the representable space of longs are valid.  This exception can
 * be thrown when an invalid long ID is encountered.
 */
public class InvalidIDException extends JanusException {

    public InvalidIDException(String msg) {
        super(msg);
    }

    public InvalidIDException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public InvalidIDException(Throwable cause) {
        super(cause);
    }
}
