package com.thinkaurelius.titan.diskstorage.locking;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;

/**
 * This exception signifies a failure to lock based on durable state. For
 * example, another machine holds the lock we attempted to claim. These
 * exceptions typically will not go away on retries unless a machine modifies
 * the underlying lock state in some way.
 * <p/>
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PermanentLockingException extends PermanentStorageException {

    private static final long serialVersionUID = 482890657293484420L;

    /**
     * @param msg Exception message
     */
    public PermanentLockingException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public PermanentLockingException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public PermanentLockingException(Throwable cause) {
        this("Permanent locking failure", cause);
    }


}
