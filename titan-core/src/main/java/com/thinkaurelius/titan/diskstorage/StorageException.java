package com.thinkaurelius.titan.diskstorage;

/**
 * Exception thrown in the storage layer of the graph database.
 * <p/>
 * Such exceptions are typically caused by the underlying storage engine and re-thrown as {@link StorageException}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public abstract class StorageException extends Exception {

    private static final long serialVersionUID = 4056436257763972423L;

    /**
     * @param msg Exception message
     */
    public StorageException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public StorageException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public StorageException(Throwable cause) {
        this("Exception in storage backend.", cause);
    }


}
