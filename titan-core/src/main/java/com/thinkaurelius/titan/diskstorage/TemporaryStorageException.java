package com.thinkaurelius.titan.diskstorage;

/**
 * This exception signifies a (potentially) temporary exception in a Titan storage backend,
 * that is, an exception that is due to a temporary unavailability or other exception that
 * is not permanent in nature.
 * <p/>
 * If this exception is thrown it indicates that retrying the same operation might potentially
 * lead to success (but not necessarily)
 * <p/>
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TemporaryStorageException extends StorageException {

    private static final long serialVersionUID = 9286719478969781L;

    /**
     * @param msg Exception message
     */
    public TemporaryStorageException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public TemporaryStorageException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public TemporaryStorageException(Throwable cause) {
        this("Temporary failure in storage backend", cause);
    }


}
