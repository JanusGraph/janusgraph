package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.*;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.thinkaurelius.titan.core.Constants;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class AstyanaxStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(AstyanaxStoreManager.class);

    //################### ASTYANAX SPECIFIC CONFIGURATION OPTIONS ######################

    /**
     * Default name for the Cassandra cluster
     * <p>
     * Value = {@value}
     */
    public static final String CLUSTER_DEFAULT = "Titan Cluster";
    public static final String CLUSTER_KEY = "cluster-name";
    
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





    private final String clusterName;
    
    private final AstyanaxContext<Keyspace> keyspaceContext;
    private final AstyanaxContext<Cluster> clusterContext;

    private final RetryPolicy retryPolicy;
    
    private final Map<String,AstyanaxOrderedKeyColumnValueStore> openStores;

	public AstyanaxStoreManager(Configuration config) throws StorageException  {
		super(config);
        
		this.clusterName = config.getString(CLUSTER_KEY, CLUSTER_DEFAULT);
		
		this.retryPolicy = getRetryPolicy(config.getString(RETRY_POLICY_KEY, RETRY_POLICY_DEFAULT));

		final int maxConnsPerHost =
				config.getInt(
						MAX_CONNECTIONS_PER_HOST_KEY,
						MAX_CONNECTIONS_PER_HOST_DEFAULT);
		
		final int maxClusterConnsPerHost =
				config.getInt(
						MAX_CLUSTER_CONNECTIONS_PER_HOST_KEY,
						MAX_CLUSTER_CONNECTIONS_PER_HOST_DEFAULT);

        AstyanaxContext.Builder ctxbuilder = getContextBuilder(config, maxConnsPerHost);
		
		final AstyanaxContext.Builder clusterCtxBuilder =
				getContextBuilder(config, maxClusterConnsPerHost);
		
		this.clusterContext = createCluster(clusterCtxBuilder);

		ensureKeyspaceExists(clusterContext.getEntity());
		
		this.keyspaceContext = ctxbuilder.buildKeyspace(ThriftFamilyFactory.getInstance());
        this.keyspaceContext.start();

        openStores = new HashMap<String,AstyanaxOrderedKeyColumnValueStore>(8);

	}

    @Override
    public Partitioner getPartitioner() throws StorageException {
        Cluster cl = clusterContext.getEntity();
        try {
            return Partitioner.getPartitioner(cl.describePartitioner());
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public String toString() {
        return "astyanax"+super.toString();
    }

	@Override
	public void close() {
		// Shutdown the Astyanax contexts
        openStores.clear();
        keyspaceContext.shutdown();
		clusterContext.shutdown();
	}

	@Override
	public synchronized AstyanaxOrderedKeyColumnValueStore openDatabase(String name) throws StorageException  {
        if (openStores.containsKey(name)) return openStores.get(name);
        else {
            ensureColumnFamilyExists(name);
            AstyanaxOrderedKeyColumnValueStore store = new AstyanaxOrderedKeyColumnValueStore(name, keyspaceContext.getEntity(), this, retryPolicy);
            openStores.put(name,store);
            return store;
        }
	}

    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, Mutation>> batch, StoreTransaction txh) throws StorageException {
        MutationBatch m = keyspaceContext.getEntity().prepareMutationBatch()
                .setConsistencyLevel(getTx(txh).getWriteConsistencyLevel().getAstyanaxConsistency())
                .withRetryPolicy(retryPolicy.duplicate());

        final long delTS = TimeUtility.getApproxNSSinceEpoch(false);
        final long addTS = TimeUtility.getApproxNSSinceEpoch(true);

        for (Map.Entry<String,Map<ByteBuffer, Mutation>> batchentry : batch.entrySet()) {
            String storeName = batchentry.getKey();
            Preconditions.checkArgument(openStores.containsKey(storeName),"Store cannot be found: " + storeName);
            ColumnFamily<ByteBuffer, ByteBuffer> columnFamily = openStores.get(storeName).getColumnFamily();

            Map<ByteBuffer, Mutation> mutations = batchentry.getValue();
            for (Map.Entry<ByteBuffer, Mutation> ent : mutations.entrySet()) {
                // The CLMs for additions and deletions are separated because
                // Astyanax's operation timestamp cannot be set on a per-delete
                // or per-addition basis.
                ColumnListMutation<ByteBuffer> dels = m.withRow(columnFamily, ent.getKey());
                dels.setTimestamp(delTS);
                ColumnListMutation<ByteBuffer> adds = m.withRow(columnFamily, ent.getKey());
                adds.setTimestamp(addTS);
    
                Mutation titanMutation = ent.getValue();
    
                if (titanMutation.hasDeletions()) {
                    for (ByteBuffer b : titanMutation.getDeletions()) {
                        dels.deleteColumn(b);
                    }
                }
    
                if (titanMutation.hasAdditions()) {
                    for (Entry e : titanMutation.getAdditions()) {
                        adds.putColumn(e.getColumn(), e.getValue(), null);
                    }
                }
            }
        }

        try {
            m.execute();
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public void clearStorage() {
        try {
            clusterContext.getEntity().dropKeyspace(keySpaceName);
        } catch (ConnectionException e) {
            log.debug("Failed to drop keyspace {}", keySpaceName);
        } finally {
            close();
        }
    }

    private void ensureColumnFamilyExists(String name) throws StorageException  {
        Cluster cl = clusterContext.getEntity();
        try {
            KeyspaceDefinition ksDef = cl.describeKeyspace(keySpaceName);
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
                                .setKeyspace(keySpaceName)
                                .setComparatorType("org.apache.cassandra.db.marshal.BytesType");
                cl.addColumnFamily(cfDef);
            }
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }
    }

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
                        .forKeyspace(keySpaceName)
                        .withAstyanaxConfiguration(
                                new AstyanaxConfigurationImpl()
                                        .setConnectionPoolType(poolType)
                                        .setDiscoveryType(discType))
                        .withConnectionPoolConfiguration(
                                new ConnectionPoolConfigurationImpl("TitanConnectionPool")
                                        .setPort(port)
                                        .setMaxConnsPerHost(maxConnsPerHost)
                                        .setRetryBackoffStrategy(new FixedRetryBackoffStrategy(1000, 5000)) // TODO configuration
                                        .setSocketTimeout(connectionTimeout)
                                        .setConnectTimeout(connectionTimeout)
                                        .setSeeds(hostname))
                        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

        return builder;
    }

    private void ensureKeyspaceExists(Cluster cl) throws StorageException  {
        KeyspaceDefinition ksDef;

        try {
            ksDef = cl.describeKeyspace(keySpaceName);

            if (null != ksDef && ksDef.getName().equals(keySpaceName)) {
                log.debug("Found keyspace {}", keySpaceName);
                return;
            }
        } catch (ConnectionException e) {
            log.debug("Failed to describe keyspace {}", keySpaceName);
        }

        log.debug("Creating keyspace {}...", keySpaceName);
        try {
            Map<String, String> stratops = new HashMap<String, String>() {{
                put("replication_factor", String.valueOf(replicationFactor));
                put(VERSION_PROPERTY_KEY, Constants.VERSION);
            }};

            ksDef = cl.makeKeyspaceDefinition()
                    .setName(keySpaceName)
                    .setStrategyClass("org.apache.cassandra.locator.SimpleStrategy")
                    .setStrategyOptions(stratops);
            cl.addKeyspace(ksDef);

            log.debug("Created keyspace {}", keySpaceName);
        } catch (ConnectionException e) {
            log.debug("Failed to create keyspace {}, keySpaceName");
            throw new TemporaryStorageException(e);
        }
    }
    
    private static RetryPolicy getRetryPolicy(String serializedRetryPolicy) throws StorageException  {
    	String[] tokens = serializedRetryPolicy.split(",");
    	String policyClassName = tokens[0];
    	int argCount = tokens.length - 1;
    	Object[] args = new Object[argCount];
    	for (int i = 1; i < tokens.length; i++) {
    		args[i-1] = Integer.valueOf(tokens[i]);
    	}
    	
    	try {
    		RetryPolicy rp = instantiateRetryPolicy(policyClassName, args, serializedRetryPolicy);
    		log.debug("Instantiated RetryPolicy object {} from config string \"{}\"", rp, serializedRetryPolicy);
    		return rp;
    	} catch (Exception e) {
    		throw new PermanentStorageException("Failed to instantiate Astyanax Retry Policy class",e);
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

    @Override
    public String getLastSeenTitanVersion() throws StorageException {
        Cluster cl = clusterContext.getEntity();

        try {
            KeyspaceDefinition ksDef = cl.describeKeyspace(keySpaceName);
            Preconditions.checkNotNull(ksDef);
            return ksDef.getStrategyOptions().get(VERSION_PROPERTY_KEY);
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public void setTitanVersionToLatest() throws StorageException {
        Cluster cl = clusterContext.getEntity();

        try {
            KeyspaceDefinition currentKs = cl.describeKeyspace(keySpaceName);
            Preconditions.checkNotNull(currentKs);

            cl.updateKeyspace(cl.makeKeyspaceDefinition()
                                    .setName(currentKs.getName())
                                    .setStrategyClass(currentKs.getStrategyClass())
                                    .setStrategyOptions(new HashMap<String, String>(currentKs.getStrategyOptions()) {{
                                        put(VERSION_PROPERTY_KEY, Constants.VERSION);
                                    }}));
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }
    }
}


