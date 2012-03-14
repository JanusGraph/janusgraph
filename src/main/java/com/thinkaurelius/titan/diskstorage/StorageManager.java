package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.exceptions.GraphStorageException;

/**
 * StorageManager provides the persistence context to the graph database middleware.
 * 
 * A StorageManager provides transaction handles across multiple data stores that
 * are managed by this StorageManager.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 */
public interface StorageManager {

	/**
	 * Opens a database by the given name. If the database does not exist, it is
	 * created. If it has already been opened, the existing handle is returned.
	 * 
	 * @param name Name of database
	 * @return Database Handle
	 * @throws GraphStorageException
	 */
	public KeyColumnValueStore openDatabase(String name) throws GraphStorageException;
	
	/**
	 * Opens an ordered database by the given name. If the database does not exist, it is
	 * created. If it has already been opened, the existing handle is returned.
	 * 
	 * @param name Name of database
	 * @return Database Handle
	 * @throws GraphStorageException
	 */
	public OrderedKeyColumnValueStore openOrderedDatabase(String name) throws GraphStorageException;
		
	/**
	 * Returns a transaction handle for a new transaction.
	 * 
	 * @return New Transaction Hanlde
	 */
	public TransactionHandle beginTransaction();
	
	/**
	 * Closes the Storage Manager and all databases that have been opened.
	 */
	public void close();
	

	
	
}
