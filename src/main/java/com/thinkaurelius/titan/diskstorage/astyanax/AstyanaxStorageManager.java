package com.thinkaurelius.titan.diskstorage.astyanax;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.HOSTNAME_DEFAULT;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.KEYSPACE_DEFAULT;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.KEYSPACE_KEY;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.PORT_DEFAULT;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.READ_CONSISTENCY_LEVEL_KEY;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.REPLICATION_FACTOR_DEFAULT;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.REPLICATION_FACTOR_KEY;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.THRIFT_TIMEOUT_DEFAULT;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.THRIFT_TIMEOUT_KEY;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager.WRITE_CONSISTENCY_LEVEL_KEY;

import java.lang.reflect.Constructor;
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
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;
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
     * Default name for the Cassandra cluster
     * <p>
     * Value = {@value}
     */
    public static final String CLUSTER_DEFAULT = "Titan Cluster";
    public static final String CLUSTER_KEY = "cluster";
    
    /**
     * Maximum pooled connections per host.
     * <p>
     * Value = {@value}
     */
    public static final int MAX_CONNECTIONS_PER_HOST_DEFAULT = 32;
    public static final String MAX_CONNECTIONS_PER_HOST_KEY = "max-connections-per-host";
    
    /**
     * Maximum pooled "cluster" connections per host.
     * 
     * These connections are mostly idle and only used for DDL operations
     * (like creating keyspaces).  Titan doesn't need many of these connections
     * in ordinary operation.
     * 
     */
    public static final int MAX_CLUSTER_CONNECTIONS_PER_HOST_DEFAULT = 3;
    public static final String MAX_CLUSTER_CONNECTIONS_PER_HOST_KEY = "max-cluster-connections-per-host";
    
	/**
	 * How Astyanax discovers Cassandra cluster nodes. This must be one of the
	 * values of the Astyanax NodeDiscoveryType enum.
	 * <p>
	 * Value = {@value}
	 */
    public static final String NODE_DISCOVERY_TYPE_DEFAULT = "RING_DESCRIBE";
    public static final String NODE_DISCOVERY_TYPE_KEY = "node-discovery-type";
    
	/**
	 * Astyanax's connection pooler implementation. This must be one of the
	 * values of the Astyanax ConnectionPoolType enum.
	 * <p>
	 * Value = {@value}
	 */
    public static final String CONNECTION_POOL_TYPE_DEFAULT = "TOKEN_AWARE";
    public static final String CONNECTION_POOL_TYPE_KEY = "connection-pool-type";
    
    /**
     * One of the values of the Astyanax ConsistencyLevel enum.  If this
     * string value doesn't start with "CL_", then it is prepended before
     * conversion into an Astyanax enum.
     * <p>
     * Value = {@value}
     */
    public static final String READ_CONSISTENCY_LEVEL_DEFAULT = "CL_QUORUM";
    
    /**
     * One of the values of the Astyanax ConsistencyLevel enum.  If this
     * string value doesn't start with "CL_", then it is prepended before
     * conversion into an Astyanax enum.
     * <p>
     * Value = {@value}
     */
    public static final String WRITE_CONSISTENCY_LEVEL_DEFAULT = "CL_QUORUM";
    
	/**
	 * This must be the fully-qualified classname (i.e. the complete package
	 * name, a dot, and then the class name) of an implementation of Astyanax's
	 * RetryPolicy interface. This string may be followed by a sequence of
	 * integers, separated from the full classname and from each other by
	 * commas; in this case, the integers are cast to native Java ints and
	 * passed to the class constructor as arguments.
	 * <p>
	 * Value = {@value}
	 */
    public static final String RETRY_POLICY_DEFAULT = "com.netflix.astyanax.retry.BoundedExponentialBackoff,25,1000,30";
    public static final String RETRY_POLICY_KEY = "retry-policy";
    
    private final AstyanaxContext<Keyspace> ksctx;
    private final AstyanaxContext<Cluster> clctx;
    private final AstyanaxContext.Builder ctxbuilder;
    
	private final String ksName;
	private final String clusterName;
	
    private final int lockRetryCount;
    private final long lockWaitMS, lockExpireMS;
    private final byte[] rid;
    private final int replicationFactor;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;
    private final RetryPolicy retryPolicy;
    
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
		
		this.replicationFactor = 
				config.getInt(
						REPLICATION_FACTOR_KEY,
						REPLICATION_FACTOR_DEFAULT);
		
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
		
		this.retryPolicy = getRetryPolicy(config);
		
		this.readConsistencyLevel = getConsistency(config,
				READ_CONSISTENCY_LEVEL_KEY, READ_CONSISTENCY_LEVEL_DEFAULT);

		this.writeConsistencyLevel = getConsistency(config,
				WRITE_CONSISTENCY_LEVEL_KEY, WRITE_CONSISTENCY_LEVEL_DEFAULT);

		final int maxConnsPerHost =
				config.getInt(
						MAX_CONNECTIONS_PER_HOST_KEY,
						MAX_CONNECTIONS_PER_HOST_DEFAULT);
		
		final int maxClusterConnsPerHost =
				config.getInt(
						MAX_CLUSTER_CONNECTIONS_PER_HOST_KEY,
						MAX_CLUSTER_CONNECTIONS_PER_HOST_DEFAULT);
		
		this.ctxbuilder = getContextBuilder(config, maxConnsPerHost);
		
		final AstyanaxContext.Builder clusterCtxBuilder =
				getContextBuilder(config, maxClusterConnsPerHost);
		
		this.clctx = createCluster(clusterCtxBuilder);

		ensureKeyspaceExists(clctx.getEntity());
		
		this.ksctx = createKeyspace();

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
		
		return new AstyanaxOrderedKeyColumnValueStore(ksctx.getEntity(), name, lcb, readConsistencyLevel, writeConsistencyLevel, retryPolicy);
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
	
	private AstyanaxContext<Keyspace> createKeyspace() {
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
	
	private static AstyanaxContext<Cluster> createCluster(AstyanaxContext.Builder cb) {
		AstyanaxContext<Cluster> clusterCtx =
				cb.buildCluster(ThriftFamilyFactory.getInstance());
		clusterCtx.start();
		
		return clusterCtx;
	}
	
	private AstyanaxContext.Builder getContextBuilder(Configuration config, int maxConnsPerHost) {
		
		final ConnectionPoolType poolType = ConnectionPoolType.valueOf(
				config.getString(
						CONNECTION_POOL_TYPE_KEY,
						CONNECTION_POOL_TYPE_DEFAULT));
		
		final NodeDiscoveryType discType = NodeDiscoveryType.valueOf(
				config.getString(
						NODE_DISCOVERY_TYPE_KEY,
						NODE_DISCOVERY_TYPE_DEFAULT));
		
		AstyanaxContext.Builder builder = 
				new AstyanaxContext.Builder()
				.forCluster(clusterName)
				.forKeyspace(ksName)
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setConnectionPoolType(poolType)
								.setDiscoveryType(discType))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("TitanConnectionPool")
								.setPort(config.getInt(PORT_KEY,PORT_DEFAULT))
                                .setMaxConnsPerHost(maxConnsPerHost)
                                .setRetryBackoffStrategy(new FixedRetryBackoffStrategy(1000, 5000)) // TODO configuration
                                .setSocketTimeout(config.getInt(THRIFT_TIMEOUT_KEY, THRIFT_TIMEOUT_DEFAULT))
                                .setConnectTimeout(config.getInt(THRIFT_TIMEOUT_KEY,THRIFT_TIMEOUT_DEFAULT))
								.setSeeds(config.getString(HOSTNAME_KEY,HOSTNAME_DEFAULT))) //"127.0.0.1:9160" or "127.0.0.1"
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
			stratops.put("replication_factor", String.valueOf(replicationFactor));
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
    
    private static ConsistencyLevel getConsistency(Configuration config, String key, String defaultValue) {
    	String raw = config.getString(key, defaultValue);
    	
    	if (!raw.startsWith("CL_")) {
    		raw = "CL_" + raw;
    	}

    	return ConsistencyLevel.valueOf(raw);
    }
    
    private static RetryPolicy getRetryPolicy(Configuration config) {
    	String raw = config.getString(RETRY_POLICY_KEY, RETRY_POLICY_DEFAULT);
    	String[] tokens = raw.split(",");
    	String policyClassName = tokens[0];
    	int argCount = tokens.length - 1;
    	Object[] args = new Object[argCount];
    	for (int i = 1; i < tokens.length; i++) {
    		args[i-1] = Integer.valueOf(tokens[i]);
    	}
    	
    	try {
    		RetryPolicy rp = instantiateRetryPolicy(policyClassName, args, raw);
    		log.debug("Instantiated RetryPolicy object {} from config string \"{}\"", rp, raw);
    		return rp;
    	} catch (Exception e) {
    		throw new GraphStorageException("Failed to instantiate Astyanax Retry Policy class",e);
    	}
    }

	private static RetryPolicy instantiateRetryPolicy(String policyClassName,
			Object[] args, String raw) throws Exception {

    	Class<?> policyClass = Class.forName(policyClassName);
    	
    	for (Constructor<?> con : policyClass.getConstructors()) {
    		Class<?>[] parameterClasses = con.getParameterTypes();
    		if (args.length == parameterClasses.length) {
    			boolean allInts = true;
    			for (Class<?> pc : parameterClasses) {
    				if (! pc.equals(int.class)) {
    					allInts = false;
    					break;
    				}
    			}
    			
    			if (!allInts) {
    				break;
    			}
    			
    			log.debug("About to instantiate class {} with {} arguments", con.toString(), args.length);
    			
    			return (RetryPolicy)con.newInstance(args);
    		}
    	}
    	
    	throw new Exception("Failed to identify a class matching the Astyanax Retry Policy config string \"" + raw + "\"");
    }
}


