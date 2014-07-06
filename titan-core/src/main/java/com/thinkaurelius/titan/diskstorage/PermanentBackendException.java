package com.thinkaurelius.titan.diskstorage;

/**
 * This exception signifies a permanent exception in a Titan storage backend,
 * that is, an exception that is due to a permanent failure while persisting
 * data.
 * <p/>
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PermanentBackendException extends BackendException {

    private static final long serialVersionUID = 203482308203400L;

    /**
     * @param msg Exception message
     */
    public PermanentBackendException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public PermanentBackendException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public PermanentBackendException(Throwable cause) {
        this("Permanent failure in storage backend", cause);
    }


}
