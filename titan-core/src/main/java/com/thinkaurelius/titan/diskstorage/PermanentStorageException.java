package com.thinkaurelius.titan.diskstorage;

/**
 * This exception signifies a permanent exception in a Titan storage backend,
 * that is, an exception that is due to a permanent failure while persisting
 * data.
 * <p/>
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PermanentStorageException extends StorageException {

    private static final long serialVersionUID = 203482308203400L;

    /**
     * @param msg Exception message
     */
    public PermanentStorageException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public PermanentStorageException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public PermanentStorageException(Throwable cause) {
        this("Permanent failure in storage backend", cause);
    }


}
