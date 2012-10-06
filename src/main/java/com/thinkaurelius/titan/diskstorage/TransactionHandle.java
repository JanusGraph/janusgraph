package com.thinkaurelius.titan.diskstorage;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface TransactionHandle {

    /**
     * Commits the transaction.
     *
     * @throws StorageException
     */
    public void commit() throws StorageException;

    /**
     * Aborts (or rolls back) the transaction.
     *
     * @throws StorageException
     */
    public void abort() throws StorageException;

    /**
     * Flushes the transction, i.e. forces all buffered writes to be send to the backend.
     *
     * @throws StorageException
     */
    public void flush() throws StorageException;

}
