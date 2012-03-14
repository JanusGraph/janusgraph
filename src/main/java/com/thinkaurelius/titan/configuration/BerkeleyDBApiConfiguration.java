package com.thinkaurelius.titan.configuration;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;

//import com.thinkaurelius.titan.diskstorage.berkeleydb.api.BerkeleyDBStorageManager;

public class BerkeleyDBApiConfiguration extends BaseBerkeleyDBConfiguration {

	/**
	 * The default size of the cache used by Berkeley DB in byte.
	 * Value = {@value}
	 */
	public static final long defaultCacheSize = 256*1024*1024; //256 MB
	
	private long cacheSize = defaultCacheSize;
	
	
	@Override
	public StorageManager getStorageManager(File directory, boolean readOnly) {
//		BerkeleyDBStorageManager storage = new BerkeleyDBStorageManager(directory,readOnly,isTransactional(),isPrivate());
//		storage.initialize(cacheSize);
//		return new KeyValueStorageManagerAdapter(storage);
        return null;
	}

	@Override
	public OrderedKeyColumnValueStore getEdgeStore(StorageManager manager) {
		Preconditions.checkArgument(manager instanceof KeyValueStorageManagerAdapter);
		return ((KeyValueStorageManagerAdapter)manager).openOrderedDatabase(StorageConfiguration.edgeStoreName, ByteBufferUtil.longSize);
	}

	@Override
	public OrderedKeyColumnValueStore getPropertyIndex(StorageManager manager) {
		Preconditions.checkArgument(manager instanceof KeyValueStorageManagerAdapter);
		return ((KeyValueStorageManagerAdapter)manager).openOrderedDatabase(StorageConfiguration.propertyIndexName);
	}

	@Override
	public void save(PropertiesConfiguration config) {
		config.setProperty("cache", cacheSize);
		super.save(config);
	}
	
	/**
	 * Sets the cache size for Berkeley DB
	 * @param size Size of the cache in bytes
	 */
	public void setCacheSize(long size) {
		verifyModifiable();
		cacheSize = size;
	}
	
	/**
	 * Returns the current cache size configuration for Berkeley DB
	 * 
	 * @return Size of the cache in bytes
	 */
	public long getCacheSize() {
		return cacheSize;
	}


	
}
