package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.core.GraphStorageException;

/**
 * StorageManager provides the persistence context to the graph database middleware.
 * 
 * A StorageManager provides transaction handles across multiple data stores that
 * are managed by this StorageManager.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 *
 */
public interface StorageManager extends IDAuthority {

    /**
     * Configuration key for the hostname or list of hostname of remote storage backend servers to connect to.
     */
    public static final String HOSTNAME_KEY = "hostname";

    /**
     * Configuration key for the port on which to connect to remote storage backend servers.
     */
    public static final String PORT_KEY = "port";

    /**
     * Configuration setting key for the lock lock mediator prefix
     */
    public static final String LOCAL_LOCK_MEDIATOR_PREFIX_KEY = "local-lock-mediator-prefix";


	/**
	 * Opens an ordered database by the given name. If the database does not exist, it is
	 * created. If it has already been opened, the existing handle is returned.
	 * 
	 * @param name Name of database
	 * @return Database Handle
	 * @throws GraphStorageException
	 */
	public OrderedKeyColumnValueStore openDatabase(String name) throws GraphStorageException;

    /**
     * Release a (sub-) range of previously acquired ids for the particular partition. Releasing the ids means that the
     * calling client no longer needs the ids and that the ids may be assigned to another client in the future.
     * Released ids can no longer be used by the client.
     *
     * TO BE IMPLEMENTED IN THE FUTURE
     *
     * @param partition Partition id for which to release the ids.
     * @param ids The released ids where ids[0] is inclusive and ids[1] is exclusive. ids[1]>ids[0].
     */
//    public void releaseIDBlock(int partition, long[] ids);

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


    /**
     * Deletes and clears all database in this storage manager.
     *
     * ATTENTION: Invoking this method will delete ALL your data!!
     */
	public void clearStorage();
	
}
