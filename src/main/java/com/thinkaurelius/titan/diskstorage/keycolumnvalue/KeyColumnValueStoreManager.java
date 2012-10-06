package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * KeyColumnValueStoreManager provides the persistence context to the graph database middleware.
 * 
 * A KeyColumnValueStoreManager provides transaction handles across multiple data stores that
 * are managed by this KeyColumnValueStoreManager.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 */
public interface KeyColumnValueStoreManager extends BufferMutationKeyColumnValueStore, StoreManager {


    /**
	 * Opens an ordered database by the given name. If the database does not exist, it is
	 * created. If it has already been opened, the existing handle is returned.
	 * 
	 * @param name Name of database
	 * @return Database Handle
	 * @throws com.thinkaurelius.titan.diskstorage.StorageException
	 */
	public KeyColumnValueStore openDatabase(String name) throws StorageException;




}
