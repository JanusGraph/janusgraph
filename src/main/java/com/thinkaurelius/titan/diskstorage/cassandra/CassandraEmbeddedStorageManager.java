package com.thinkaurelius.titan.diskstorage.cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.CFMetaData.Caching;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.util.ConfigHelper;
import com.thinkaurelius.titan.diskstorage.util.OrderedKeyColumnValueIDManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class CassandraEmbeddedStorageManager implements StorageManager {

	private final String keyspace;

    private final OrderedKeyColumnValueIDManager idmanager;
    
    private final int lockRetryCount;
    
    private final long lockWaitMS, lockExpireMS;
    
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;
    
    private final String llmPrefix;
    
    private final byte[] rid;
    
    private final int replicationFactor;
    
    private final String cassandraConfigDir;
    
    private static final Logger log =
    		LoggerFactory.getLogger(CassandraEmbeddedStorageManager.class);
    
    public static ConcurrentHashMap<String, CassandraEmbeddedOrderedKeyColumnValueStore> stores =
    		new ConcurrentHashMap<String, CassandraEmbeddedOrderedKeyColumnValueStore>();
    
	/**
	 * When non-empty, the CassandraEmbeddedStorageManager constructor will copy
	 * the value to the "cassandra.config" system property and start a
	 * backgrounded cassandra daemon thread. cassandra's static initializers
	 * will interpret the "cassandra.config" system property as a url pointing
	 * to a cassandra.yaml file.
	 * <p>
	 * An example value of this variable is
	 * "file:///home/dalaro/titan/target/cassandra-tmp/conf/127.0.0.1/cassandra.yaml".
	 * <p>
	 * When empty, the constructor does none of the steps described above.
	 * <p>
	 * The constructor logic described above is also internally synchronized in
	 * order to start Cassandra at most once in a thread-safe manner. Subsequent
	 * constructor invocations (or concurrent invocations which enter the
	 * internal synchronization block after the first) with a nonempty value for
	 * this variable will behave as though an empty value was set.
	 * <p>
	 * Value = {@value}
	 */
    public static final String CASSANDRA_CONFIG_DIR_DEFAULT = "";
    public static final String CASSANDRA_CONFIG_DIR_KEY = "cassandra-config-dir";
    
	public CassandraEmbeddedStorageManager(Configuration config) {
		
		this.cassandraConfigDir =
				config.getString(
						CASSANDRA_CONFIG_DIR_KEY,
						CASSANDRA_CONFIG_DIR_DEFAULT);

        
        if (null != cassandraConfigDir && !cassandraConfigDir.isEmpty()) {
        	CassandraDaemonWrapper.start(cassandraConfigDir);
        }
		
		this.rid = ConfigHelper.getRid(config);
		
		this.keyspace = config.getString(
				CassandraThriftStorageManager.KEYSPACE_KEY,
				CassandraThriftStorageManager.KEYSPACE_DEFAULT);
				
		this.llmPrefix =
				config.getString(
						LOCAL_LOCK_MEDIATOR_PREFIX_KEY,
						getClass().getName());
		
		this.replicationFactor = 
				config.getInt(
						CassandraThriftStorageManager.REPLICATION_FACTOR_KEY,
						CassandraThriftStorageManager.REPLICATION_FACTOR_DEFAULT);
		
		this.lockRetryCount =
				config.getInt(
						GraphDatabaseConfiguration.LOCK_RETRY_COUNT,
						GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT);
		
		this.lockWaitMS =
				config.getLong(
						GraphDatabaseConfiguration.LOCK_WAIT_MS,
						GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT);
		
		this.lockExpireMS =
				config.getLong(
						GraphDatabaseConfiguration.LOCK_EXPIRE_MS,
						GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT);
		
		this.readConsistencyLevel = ConsistencyLevel.valueOf(
				config.getString(
						CassandraThriftStorageManager.READ_CONSISTENCY_LEVEL_KEY,
						CassandraThriftStorageManager.READ_CONSISTENCY_LEVEL_DEFAULT));
		
		log.debug("Set read consistency level to {}", this.readConsistencyLevel);
		
		this.writeConsistencyLevel = ConsistencyLevel.valueOf(
				config.getString(
						CassandraThriftStorageManager.WRITE_CONSISTENCY_LEVEL_KEY,
						CassandraThriftStorageManager.WRITE_CONSISTENCY_LEVEL_DEFAULT));
		
		log.debug("Set write consistency level to {}", this.writeConsistencyLevel);
		
        idmanager = new OrderedKeyColumnValueIDManager(
        		openDatabase("titan_ids", keyspace, null, null), rid, config);
	}
	
	@Override
	public long[] getIDBlock(int partition) {
        return idmanager.getIDBlock(partition);
	}

	@Override
	public OrderedKeyColumnValueStore openDatabase(String name)
			throws GraphStorageException {
		
		CassandraEmbeddedOrderedKeyColumnValueStore lockStore =
				openDatabase(name + "_locks", keyspace, null, null);
		LocalLockMediator llm = LocalLockMediators.INSTANCE.get(llmPrefix + ":" + name);
		CassandraEmbeddedOrderedKeyColumnValueStore dataStore =
				openDatabase(name, keyspace, lockStore, llm);
		
		return dataStore;
	}

	@Override
	public TransactionHandle beginTransaction() {
		return new CassandraETransaction();
	}

	@Override
	public void close() {
		stores.clear();
	}

	@Override
	public void clearStorage() {
		stores.clear();
		try {
			MigrationManager.announceKeyspaceDrop(keyspace);
		} catch (ConfigurationException e) {
			throw new GraphStorageException(e);
		}
	}

	private CassandraEmbeddedOrderedKeyColumnValueStore openDatabase(
			final String name, final String ksoverride,
			CassandraEmbeddedOrderedKeyColumnValueStore lockStore,
			LocalLockMediator llm) throws GraphStorageException {
		String storeKey = llmPrefix + ":" + ksoverride + ":" + name;
		
		CassandraEmbeddedOrderedKeyColumnValueStore store =
				stores.get(storeKey);
		
		if (null != store) {
			log.debug("Retrieved existing store {}", store);
			return store;
		}
		
		// Ensure that both the keyspace and column family exist
		ensureKeyspaceExists(ksoverride);
		ensureColumnFamilyExists(ksoverride, name);
		
		store = new CassandraEmbeddedOrderedKeyColumnValueStore(ksoverride,
				name, readConsistencyLevel, writeConsistencyLevel, lockStore,
				llm, rid, lockRetryCount, lockWaitMS, lockExpireMS);
		stores.putIfAbsent(storeKey, store);
		store = stores.get(storeKey);
		log.debug("Created store {}", store);
		
		return store;
	}

	private void ensureKeyspaceExists(String name) {
		
		if (null != Schema.instance.getTableInstance(name))
			return;
		
		// Keyspace not found; create it
		String strategyName = "org.apache.cassandra.locator.SimpleStrategy";
		Map<String, String> options = new HashMap<String, String>();
		options.put("replication_factor", String.valueOf(replicationFactor));
		KSMetaData ksm;
		try {
			ksm = KSMetaData.newKeyspace(name, strategyName, options);
		} catch (ConfigurationException e) {
			throw new GraphStorageException("Failed to instantiate keyspace metadata for " + name, e);
		}
		try {
			MigrationManager.announceNewKeyspace(ksm);
		} catch (ConfigurationException e) {
			throw new GraphStorageException("Failed to create keyspace " + name, e);
		}
	}
	
	private void ensureColumnFamilyExists(String ksname, String cfname) {
		if (null != Schema.instance.getCFMetaData(ksname, cfname))
			return;
		
		// Column Family not found; create it
		CFMetaData cfm = new CFMetaData(ksname, cfname, ColumnFamilyType.Standard, BytesType.instance, null);
		
		// Hard-coded caching settings
		if (cfname.equals(GraphDatabaseConfiguration.STORAGE_EDGESTORE_NAME)) {
			cfm.caching(Caching.KEYS_ONLY);
		} else if (cfname.equals(GraphDatabaseConfiguration.STORAGE_PROPERTYINDEX_NAME)) {
			cfm.caching(Caching.ROWS_ONLY);
		}
		
		try {
			cfm.addDefaultIndexNames();
		} catch (ConfigurationException e) {
			throw new GraphStorageException("Failed to create column family metadata for " + ksname + ":" + cfname, e);
		}
		try {
			MigrationManager.announceNewColumnFamily(cfm);
		} catch (ConfigurationException e) {
			throw new GraphStorageException("Failed to create column family " + ksname + ":" + cfname, e);
		}
	}
}
