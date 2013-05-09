package com.thinkaurelius.titan.diskstorage;

/**
 * Represents a transaction for a particular storage backend.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface TransactionHandle {

    /**
     * Commits the transaction.  This implies a {@link #flush()}.
     * 
     * Call either this method or {@link #rollback()} at most once per instance.
     *
     * @throws StorageException
     */
    public void commit() throws StorageException;

    /**
     * Aborts (or rolls back) the transaction.
     * 
     * Call either this method or {@link #commit()} at most once per instance.
     *
     * @throws StorageException
     */
    public void rollback() throws StorageException;

    /**
     * Forces all buffered writes to be sent to underlying storage.
     * 
     * It is not necessary or meaningful to call this method on a
     * {@code TransactionHandle} instance after calling {@link #commit()} or
     * {@link #rollback()} on the instance.
     * 
     * @throws StorageException
     */
    public void flush() throws StorageException;

    /**
     * No-op transaction. This transaction's methods return without doing
     * anything.
     */
    public static final TransactionHandle NO_TRANSACTION = new TransactionHandle() {
        @Override
        public void commit() throws StorageException {
        }

        @Override
        public void rollback() throws StorageException {
        }

        @Override
        public void flush() throws StorageException {
        }
    };

}
