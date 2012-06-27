package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

public interface KeyValueStorageManager {

	/**
	 * Opens an ordered database by the given name. If the database does not exist, it is
	 * created. If it has already been opened, the existing handle is returned.
	 * 
	 * @param name Name of database
	 * @return Database Handle
	 * @throws GraphStorageException
	 */
	public OrderedKeyValueStore openDatabase(String name) throws GraphStorageException;


    /**
     * As defined in {@see com.thinkaurelius.titan.diskstorage.IDAuthority.getIDBlock(int,int)}
     *
     * @param partition Partition for which to request an id block.
     * @return a range of ids for the particular partition
     */
    public long[] getIDBlock(int partition);


	/**
	 * Returns a transaction handle for a new transaction.
	 * @return New Transaction Hanlde
	 */
	public TransactionHandle beginTransaction();
	
	/**
	 * Closes the Storage Manager and all databases that have been opened.
	 */
	public void close();

    /**
     * Deletes and clears all database in this storage manager.
     *
     * ATTENTION: Invoking this method will delete ALL your data!!
     */
    public void clearStorage();
	
	
}
