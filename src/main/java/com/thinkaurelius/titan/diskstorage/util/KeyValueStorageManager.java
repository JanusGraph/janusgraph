package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

public interface KeyValueStorageManager extends IDAuthority {

	/**
	 * Opens an ordered database by the given name. If the database does not exist, it is
	 * created. If it has already been opened, the existing handle is returned.
	 * 
	 * @param name Name of database
	 * @return Database Handle
	 * @throws com.thinkaurelius.titan.diskstorage.StorageException
	 */
	public OrderedKeyValueStore openDatabase(String name) throws StorageException;

	/**
	 * Returns a transaction handle for a new transaction.
	 * @return New Transaction Hanlde
	 */
	public TransactionHandle beginTransaction() throws StorageException;
	
	/**
	 * Closes the Storage Manager and all databases that have been opened.
	 */
	public void close() throws StorageException;

    /**
     * Deletes and clears all database in this storage manager.
     *
     * ATTENTION: Invoking this method will delete ALL your data!!
     */
    public void clearStorage() throws StorageException;
	
	
}
