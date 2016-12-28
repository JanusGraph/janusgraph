package com.thinkaurelius.titan.diskstorage;

/**
 * Represents a transaction for a particular storage backend.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface BaseTransaction {

    /**
     * Commits the transaction and persists all modifications to the backend.
     * 
     * Call either this method or {@link #rollback()} at most once per instance.
     *
     * @throws BackendException
     */
    public void commit() throws BackendException;

    /**
     * Aborts (or rolls back) the transaction.
     * 
     * Call either this method or {@link #commit()} at most once per instance.
     *
     * @throws BackendException
     */
    public void rollback() throws BackendException;

}
