package com.thinkaurelius.titan.diskstorage.astyanax;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.*;

public class AstyanaxStorageManager implements StorageManager {
	    
    /**
     * Default name for the Cassandra cluster
     * <p>
     * Value = {@value}
     */
    public static final String CLUSTER_DEFAULT = "Titan Cluster";
    public static final String CLUSTER_KEY = "cluster";
	
    private final AstyanaxContext<Keyspace> ksctx;
    private final AstyanaxContext<Cluster> clctx;
    private final AstyanaxContext.Builder ctxbuilder;
    
	private final String ksName;
	private final String clusterName;
	
    private final int lockRetryCount;
    private final long lockWaitMS, lockExpireMS;
    private final byte[] rid;
    
    private final OrderedKeyColumnValueIDManager idmanager;
	
	private final String llmPrefix;
	
	private static final Logger log = LoggerFactory.getLogger(AstyanaxStorageManager.class);
	
	public AstyanaxStorageManager(Configuration config) {
		
		this.clusterName = config.getString(CLUSTER_KEY, CLUSTER_DEFAULT);
		
		this.ksName = config.getString(KEYSPACE_KEY, KEYSPACE_DEFAULT);
		
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

		this.ctxbuilder = getContextBuilder(config);
		
		this.clctx = getOrCreateCluster();

		ensureKeyspaceExists(clctx.getEntity());
		
		this.ksctx = getOrCreateKeyspace();

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
		
		getOrCreateKeyspace();
		
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
		// Shutdown the Astyanax contexts
		ksctx.shutdown();
		clctx.shutdown();
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
		
		return new AstyanaxOrderedKeyColumnValueStore(ksctx.getEntity(), name, lcb);
	}
	
	private void ensureColumnFamilyExists(String name) {
		Cluster cl = clctx.getEntity();
		try {
			KeyspaceDefinition ksDef = cl.describeKeyspace(ksName);
			boolean found = false;
			if (null != ksDef) {
				for (ColumnFamilyDefinition cfDef : ksDef.getColumnFamilyList()) {
					found |= cfDef.getName().equals(name);
				}
			}
			if (!found) {
				ColumnFamilyDefinition cfDef = 
						cl.makeColumnFamilyDefinition()
						.setName(name)
						.setKeyspace(ksName)
						.setComparatorType("org.apache.cassandra.db.marshal.BytesType");
				cl.addColumnFamily(cfDef);
			}
		} catch (ConnectionException e) {
			throw new GraphStorageException(e);
		}
	}
	
	private AstyanaxContext<Keyspace> getOrCreateKeyspace() {
		AstyanaxContext<Keyspace> ksctx = 
				ctxbuilder.buildKeyspace(ThriftFamilyFactory.getInstance());
		ksctx.start();
		
		return ksctx;
	}
	
//	private AstyanaxContext<Cluster> getOrCreateCluster(String clusterName, AstyanaxContext.Builder builder) {
//		AstyanaxContext<Cluster> ctx =
//				builder.buildCluster(ThriftFamilyFactory.getInstance());
//		ctx.start();
//		if (null != clusters.putIfAbsent(clusterName, ctx)) {
//			ctx.shutdown();
//		}
//		return clusters.get(clusterName);
//	}
	
	private AstyanaxContext<Cluster> getOrCreateCluster() {
		AstyanaxContext<Cluster> clusterCtx =
				ctxbuilder.buildCluster(ThriftFamilyFactory.getInstance());
		clusterCtx.start();
		
		return clusterCtx;
	}
	
	private AstyanaxContext.Builder getContextBuilder(Configuration config) {
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
								.setPort(config.getInt(PORT_KEY,PORT_DEFAULT))
                                .setMaxConnsPerHost(16)
                                .setConnectTimeout(config.getInt(THRIFT_TIMEOUT_KEY,THRIFT_TIMEOUT_DEFAULT))
								.setSeeds(config.getString(HOSTNAME_KEY,HOSTNAME_DEFAULT))) //"127.0.0.1:9160"
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor());
		
		return builder;
	}
	
	private void ensureKeyspaceExists(Cluster cl) {
		KeyspaceDefinition ksDef;
		
		try {
			ksDef = cl.describeKeyspace(ksName);
			
			if (null != ksDef && ksDef.getName().equals(ksName)) {
				log.debug("Found keyspace {}", ksName);
				return;
			}
		} catch (ConnectionException e) {
			log.debug("Failed to describe keyspace {}", ksName);
		}

		log.debug("Creating keyspace {}...", ksName);		
		try {
			Map<String, String> stratops = new HashMap<String, String>();
			stratops.put("replication_factor", "1"); //TODO config
			ksDef = cl.makeKeyspaceDefinition()
					.setName(ksName)
					.setStrategyClass("org.apache.cassandra.locator.SimpleStrategy")
					.setStrategyOptions(stratops);
			cl.addKeyspace(ksDef);
			
			log.debug("Created keyspace {}", ksName);
		} catch (ConnectionException e) {
			log.debug("Failed to create keyspace {}, ksName");
			throw new GraphStorageException(e);
		}
	}


    @Override
    public void clearStorage() {
        try {
            clctx.getEntity().dropKeyspace(ksName);
        } catch (ConnectionException e) {
            log.debug("Failed to drop keyspace {}", ksName);
        } finally {
            close();
        }
    }
}


