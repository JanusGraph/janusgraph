package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransactionHandle;

public interface KeyValueStoreManager extends StoreManager {

	/**
	 * Opens an ordered database by the given name. If the database does not exist, it is
	 * created. If it has already been opened, the existing handle is returned.
     *
     * By default, the key length will be variable
	 * 
	 * @param name Name of database
	 * @return Database Handle
	 * @throws com.thinkaurelius.titan.diskstorage.StorageException
	 */
	public KeyValueStore openDatabase(String name) throws StorageException;

	
}
