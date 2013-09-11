package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * Exception thrown by {@link CacheStore#replace(com.thinkaurelius.titan.diskstorage.StaticBuffer, com.thinkaurelius.titan.diskstorage.StaticBuffer, com.thinkaurelius.titan.diskstorage.StaticBuffer, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction)}
 * if the replace failed due to a mismatch in old value.
 * <p/>
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class CacheUpdateException extends StorageException {

    /**
     * @param msg Exception message
     */
    public CacheUpdateException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public CacheUpdateException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public CacheUpdateException(Throwable cause) {
        this("Exception in storage backend.", cause);
    }
}
