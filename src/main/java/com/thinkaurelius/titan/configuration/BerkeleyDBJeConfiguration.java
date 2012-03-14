package com.thinkaurelius.titan.configuration;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyDBStorageManager;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;

/**
 * Configuration of the Berkeley DB Java Edition storage backend.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public class BerkeleyDBJeConfiguration extends BaseBerkeleyDBConfiguration {

	/**
	 * The default percentage of the JVM heap size to be used as cache for Berkeley DB
	 * Value = {@value}
	 */
	public static final int defaultCachePercentage = 65;

	private int cachePercent = defaultCachePercentage;
	
	@Override
	public StorageManager getStorageManager(File directory, boolean readOnly) {
		BerkeleyDBStorageManager storage = new BerkeleyDBStorageManager(getDirectory(directory),readOnly,isTransactional(), isBatchLoading());
		storage.initialize(cachePercent);
		return new KeyValueStorageManagerAdapter(storage);
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
		config.setProperty("cache", cachePercent);
		super.save(config);
	}
	
	/**
	 * Sets the percentage of the JVM heap size to be used by Berkeley DB for its database cache
	 * 
	 * @param percent JVM heap percentage to use for cache
	 */
	public void setCachePercent(int percent) {
		verifyModifiable();
		cachePercent = percent;
	}
	
	/**
	 * Returns the configured percentage of the JVM heap size to be used by Berkeley DB for its database cache
	 * @return Percentage of JVM heap size used as database cache
	 */
	public int getCachePercent() {
		return cachePercent;
	}


}
