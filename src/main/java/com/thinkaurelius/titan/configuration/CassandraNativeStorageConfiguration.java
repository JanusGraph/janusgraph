package com.thinkaurelius.titan.configuration;

import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
//import com.thinkaurelius.titan.diskstorage.cassandra.direct.CassandraBinaryStorageManager;
import com.thinkaurelius.titan.net.NodeID2InetMapper;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CassandraNativeStorageConfiguration extends AbstractStorageConfiguration {
	
	private static final Logger log =
		LoggerFactory.getLogger(CassandraNativeStorageConfiguration.class);
	
	private static final String PROP_KEYSPACE = "cassandra.native.keyspace";
	
	/**
	 * Default name for the Cassandra keyspace
	 * Value = {@value}
	 */
	public static final String DEFAULT_KEYSPACE = "titantest00";
	
	private String keyspace = DEFAULT_KEYSPACE;
	
	public void setKeyspace(String keyspace) {
		verifyModifiable();
		this.keyspace = keyspace;
	}
	
	public String getKeyspace() {
		return keyspace;
	}
	
	@Override
	public StorageManager getStorageManager(File directory, boolean readOnly) {
		if (readOnly)
			log.warn("Ignoring argument \"readOnly\"; only read-write "
					+ "access is currently supported by the Cassandra Thrift "
					+ "interface.");
		
//		return new CassandraBinaryStorageManager(keyspace);
        return null;
	}

	@Override
	public OrderedKeyColumnValueStore getEdgeStore(StorageManager manager) {
		return manager.openOrderedDatabase(StorageConfiguration.edgeStoreName);
	}

	@Override
	public OrderedKeyColumnValueStore getPropertyIndex(StorageManager manager) {
		return manager.openOrderedDatabase(StorageConfiguration.propertyIndexName);
	}

	@Override
	public NodeID2InetMapper getNodeIDMapper() {
		log.warn("getNodeIDMapper() not yet implemented"); // TODO
		return null;
	}

	@Override
	public void save(PropertiesConfiguration config) {
		config.setProperty(PROP_KEYSPACE, keyspace);
	}
	
	@Override
	public void open(PropertiesConfiguration config) {
		super.open(config);
		this.keyspace = config.getString(PROP_KEYSPACE, DEFAULT_KEYSPACE);
	}
	
	/**
	 * Just for testing purposes.  This method drops the two
	 * column families used by Titan inside its configured keyspace.
	 * This destroys all stored data!
	 */
	public void dropAllStorage() {
//		CassandraBinaryStorageManager m =
//			new CassandraBinaryStorageManager(keyspace);
//		m.dropDatabase(StorageConfiguration.edgeStoreName);
//		m.dropDatabase(StorageConfiguration.propertyIndexName);
	}

}
