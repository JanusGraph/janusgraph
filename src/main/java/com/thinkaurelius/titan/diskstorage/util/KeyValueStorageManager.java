package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

public interface KeyValueStorageManager {

	/**
	 * Opens a database by the given name. If the database does not exist, it is
	 * created. If it has already been opened, the existing handle is returned.
	 * 
	 * @param name Name of database
	 * @return Database Handle
	 * @throws GraphStorageException
	 */
	public KeyValueStore openDatabase(String name) throws GraphStorageException;
	
	/**
	 * Opens an ordered database by the given name. If the database does not exist, it is
	 * created. If it has already been opened, the existing handle is returned.
	 * 
	 * @param name Name of database
	 * @return Database Handle
	 * @throws GraphStorageException
	 */
	public OrderedKeyValueStore openOrderedDatabase(String name) throws GraphStorageException;
		
	/**
	 * Returns a transaction handle for a new transaction.
	 * @return New Transaction Hanlde
	 */
	public TransactionHandle beginTransaction();
	
	/**
	 * Closes the Storage Manager and all databases that have been opened.
	 */
	public void close();
	

	
	
}
