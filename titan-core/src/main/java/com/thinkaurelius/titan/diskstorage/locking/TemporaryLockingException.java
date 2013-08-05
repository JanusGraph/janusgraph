package com.thinkaurelius.titan.diskstorage.locking;

import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;

/**
 * This exception signifies a (potentially) temporary exception while attempting to acquire a lock
 * in the Titan storage backend.
 * <p/>
 * <p/>
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TemporaryLockingException extends TemporaryStorageException {

    private static final long serialVersionUID = 482890657293484420L;

    /**
     * @param msg Exception message
     */
    public TemporaryLockingException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public TemporaryLockingException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public TemporaryLockingException(Throwable cause) {
        this("Temporary locking failure", cause);
    }


}
