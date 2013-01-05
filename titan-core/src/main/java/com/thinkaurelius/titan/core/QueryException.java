package com.thinkaurelius.titan.core;

/**
 * Exception thrown when a user defined query is invalid or could not be processed.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public class QueryException extends TitanException {

    private static final long serialVersionUID = 1L;

    /**
     * @param msg Exception message
     */
    public QueryException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public QueryException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public QueryException(Throwable cause) {
        this("Exception in query.", cause);
    }

}
