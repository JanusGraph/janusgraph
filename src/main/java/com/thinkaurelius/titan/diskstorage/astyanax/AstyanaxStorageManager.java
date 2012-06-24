package com.thinkaurelius.titan.diskstorage.astyanax;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.Configuration;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.util.ConfigHelper;
import com.thinkaurelius.titan.diskstorage.util.OrderedKeyColumnValueIDManager;
import com.thinkaurelius.titan.diskstorage.util.SimpleLockConfig;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class AstyanaxStorageManager implements StorageManager {
	
    /**
     * Default name for the Cassandra keyspace
     * <p>
     * Value = {@value}
     */
    public static final String KEYSPACE_DEFAULT = "titan";
    public static final String KEYSPACE_KEY = "keyspace";
    
    /**
     * Default name for the Cassandra cluster
     * <p>
     * Value = {@value}
     */
    public static final String CLUSTER_DEFAULT = "Test Cluster";
    public static final String CLUSTER_KEY = "cluster";
	
	private static final ConcurrentHashMap<String, AstyanaxContext<Keyspace>> keyspaces =
			new ConcurrentHashMap<String, AstyanaxContext<Keyspace>>();
	
	private static final ConcurrentHashMap<String, AstyanaxContext<Cluster>> clusters =
			new ConcurrentHashMap<String, AstyanaxContext<Cluster>>();
	
	private final AstyanaxContext<Keyspace> ks;
	private final String ksName;
	private final String clusterName;
	
    private final int lockRetryCount;
    private final long lockWaitMS, lockExpireMS;
    private final byte[] rid;
    
    private final OrderedKeyColumnValueIDManager idmanager;
	
	private final String llmPrefix;
	
	public AstyanaxStorageManager(Configuration config) {
		
		this.clusterName = config.getString(CLUSTER_KEY, CLUSTER_DEFAULT);
		
		this.ksName = config.getString(KEYSPACE_KEY, KEYSPACE_DEFAULT);
		
		this.ks = getOrCreateKeyspace();
		
		this.rid = ConfigHelper.getRid(config);
		
		this.llmPrefix =
				config.getString(
						LOCAL_LOCK_MEDIATOR_PREFIX_KEY,
						getClass().getName());
		
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
		
        idmanager = new OrderedKeyColumnValueIDManager(
        		openDatabase("titan_ids", null), rid, config);
	}
	
	@Override
	public long[] getIDBlock(int partition) {
        return idmanager.getIDBlock(partition);
	}

	@Override
	public OrderedKeyColumnValueStore openDatabase(String name)
			throws GraphStorageException {
		AstyanaxOrderedKeyColumnValueStore lockStore =
				openDatabase(name + "_locks", null);
		LocalLockMediator llm = LocalLockMediators.INSTANCE.get(llmPrefix + ":" + ksName + ":" + name);
		
		return openDatabase(name, makeLockConfigBuilder(lockStore, llm));
	}

	@Override
	public TransactionHandle beginTransaction() {
		return new AstyanaxTransaction();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	
	// TODO remove
	public static void clearKeyspaces() {
		keyspaces.clear();
	}
	
	private SimpleLockConfig.Builder makeLockConfigBuilder(AstyanaxOrderedKeyColumnValueStore lockStore, LocalLockMediator llm) {
		return (new SimpleLockConfig.Builder())
			.localLockMediator(llm)
			.lockExpireMS(lockExpireMS)
			.lockRetryCount(lockRetryCount)
			.lockStore(lockStore)
			.lockWaitMS(lockWaitMS)
			.rid(rid);
	}
	
	private AstyanaxOrderedKeyColumnValueStore openDatabase(String name, SimpleLockConfig.Builder lcb) {

		ensureColumnFamilyExists(name);
		
		return new AstyanaxOrderedKeyColumnValueStore(ks.getEntity(), name, lcb);
	}
	
	private void ensureColumnFamilyExists(String name) {
		
		Cluster cl = clusters.get(clusterName).getEntity();
		
		try {
			KeyspaceDefinition ksDef = cl.describeKeyspace(ksName);
			
			boolean found = false;
			
			if (null != ksDef) {
				for (ColumnFamilyDefinition cfDef : ksDef.getColumnFamilyList()) {
					found |= cfDef.getName().equals(name);
				}
			}

			if (!found) {
				ColumnFamilyDefinition cfDef = cl.makeColumnFamilyDefinition()
						.setName(name).setKeyspace(ksName).setComparatorType("org.apache.cassandra.db.marshal.BytesType");
				
				cl.addColumnFamily(cfDef);
			}
		} catch (ConnectionException e) {
			throw new GraphStorageException(e);
		}
	}
	
	private AstyanaxContext<Keyspace> getOrCreateKeyspace() {
		AstyanaxContext<Keyspace> ks = keyspaces.get(ksName);
		
		if (null != ks)
			return ks;

		// TODO actual configuration
		AstyanaxContext.Builder builder = 
				new AstyanaxContext.Builder()
				.forCluster(clusterName)
				.forKeyspace(ksName)
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.NONE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(9160).setMaxConnsPerHost(16)
								.setSeeds("127.0.0.1:9160"))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor());
		
		ks = builder.buildKeyspace(ThriftFamilyFactory.getInstance());

		ks.start();
		
		if (null != keyspaces.putIfAbsent(ksName, ks)) {
			ks.shutdown();
		} else {
			AstyanaxContext<Cluster> clusterCtx = builder.buildCluster(ThriftFamilyFactory.getInstance());
			
			clusterCtx.start();
			
			clusters.putIfAbsent(clusterName, clusterCtx);
			
			clusterCtx = clusters.get(clusterName);
			
			Cluster cl = clusterCtx.getEntity();
			
			Map<String, String> stratops = new HashMap<String, String>();
			stratops.put("replication_factor", "1"); //TODO
			
			KeyspaceDefinition ksDef = cl.makeKeyspaceDefinition()
				.setName(ksName)
				.setStrategyClass("org.apache.cassandra.locator.SimpleStrategy")
				.setStrategyOptions(stratops);
			
			try {
				cl.addKeyspace(ksDef);
			} catch (ConnectionException e) {
				throw new GraphStorageException(e);
			}
		}

		return keyspaces.get(ksName);
	}
}


