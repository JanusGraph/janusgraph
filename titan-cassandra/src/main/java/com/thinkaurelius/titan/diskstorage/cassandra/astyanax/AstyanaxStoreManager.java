package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.*;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.*;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.astyanax.locking.AstyanaxRecipeLocker;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NS;

public class AstyanaxStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(AstyanaxStoreManager.class);

    //################### ASTYANAX SPECIFIC CONFIGURATION OPTIONS ######################

    /**
     * Default name for the Cassandra cluster
     * <p/>
     */
    public static final ConfigOption<String> CLUSTER_NAME = new ConfigOption<String>(STORAGE_NS,"cluster-name",
            "Default name for the Cassandra cluster",
            ConfigOption.Type.MASKABLE, "Titan Cluster");

    /**
     * Maximum pooled connections per host.
     * <p/>
     */
    public static final ConfigOption<Integer> MAX_CONNECTIONS_PER_HOST = new ConfigOption<Integer>(STORAGE_NS,"max-connections-per-host",
            "Maximum pooled connections per host",
            ConfigOption.Type.MASKABLE, 32);
//    public static final int MAX_CONNECTIONS_PER_HOST_DEFAULT = 32;
//    public static final String MAX_CONNECTIONS_PER_HOST_KEY = "max-connections-per-host";

    /**
     * Maximum open connections allowed in the pool (counting all hosts).
     * <p/>
     */
    public static final ConfigOption<Integer> MAX_CONNECTIONS = new ConfigOption<Integer>(STORAGE_NS,"max-connections",
            "Maximum open connections allowed in the pool (counting all hosts)",
            ConfigOption.Type.MASKABLE, -1);
//    public static final int MAX_CONNECTIONS_DEFAULT = -1;
//    public static final String MAX_CONNECTIONS_KEY = "max-connections";

    /**
     * Maximum number of operations allowed per connection before the connection is closed.
     * <p/>
     */
    public static final ConfigOption<Integer> MAX_OPERATIONS_PER_CONNECTION = new ConfigOption<Integer>(STORAGE_NS,"max-operations-per-connection",
            "Maximum number of operations allowed per connection before the connection is closed",
            ConfigOption.Type.MASKABLE, 100*1000);
//    public static final int MAX_OPERATIONS_PER_CONNECTION_DEFAULT = 100 * 1000;
//    public static final String MAX_OPERATIONS_PER_CONNECTION_KEY = "max-operations-per-connection";

    /**
     * Maximum pooled "cluster" connections per host.
     * <p/>
     * These connections are mostly idle and only used for DDL operations
     * (like creating keyspaces).  Titan doesn't need many of these connections
     * in ordinary operation.
     */
    public static final ConfigOption<Integer> MAX_CLUSTER_CONNECTIONS_PER_HOST = new ConfigOption<Integer>(STORAGE_NS,"max-cluster-connections-per-host",
            "Maximum pooled \"cluster\" connections per host",
            ConfigOption.Type.MASKABLE, 3);
//    public static final int MAX_CLUSTER_CONNECTIONS_PER_HOST_DEFAULT = 3;
//    public static final String MAX_CLUSTER_CONNECTIONS_PER_HOST_KEY = "max-cluster-connections-per-host";

    /**
     * How Astyanax discovers Cassandra cluster nodes. This must be one of the
     * values of the Astyanax NodeDiscoveryType enum.
     * <p/>
     */
    public static final ConfigOption<String> NODE_DISCOVERY_TYPE = new ConfigOption<String>(STORAGE_NS,"node-discovery-type",
            "How Astyanax discovers Cassandra cluster nodes",
            ConfigOption.Type.MASKABLE, "RING_DESCRIBE");
//    public static final String NODE_DISCOVERY_TYPE_DEFAULT = "RING_DESCRIBE";
//    public static final String NODE_DISCOVERY_TYPE_KEY = "node-discovery-type";

    /**
     * Astyanax's connection pooler implementation. This must be one of the
     * values of the Astyanax ConnectionPoolType enum.
     * <p/>
     */
    public static final ConfigOption<String> CONNECTION_POOL_TYPE = new ConfigOption<String>(STORAGE_NS,"connection-pool-type",
            "Astyanax's connection pooler implementation",
            ConfigOption.Type.MASKABLE, "TOKEN_AWARE");
//    public static final String CONNECTION_POOL_TYPE_DEFAULT = "TOKEN_AWARE";
//    public static final String CONNECTION_POOL_TYPE_KEY = "connection-pool-type";

    /**
     * In Astyanax, RetryPolicy and RetryBackoffStrategy sound and look similar
     * but are used for distinct purposes. RetryPolicy is for retrying failed
     * operations. RetryBackoffStrategy is for retrying attempts to talk to
     * uncommunicative hosts. This config option controls RetryPolicy.
     */
    public static final ConfigOption<String> RETRY_POLICY = new ConfigOption<String>(STORAGE_NS,"retry-policy",
            "Astyanax's retry policy implementation with configuration parameters",
            ConfigOption.Type.MASKABLE, "com.netflix.astyanax.retry.BoundedExponentialBackoff,100,25000,8");
//    public static final String RETRY_POLICY_DEFAULT = "com.netflix.astyanax.retry.BoundedExponentialBackoff,100,25000,8";
//    public static final String RETRY_POLICY_KEY = "retry-policy";

    /**
     * If non-null, this must be the fully-qualified classname (i.e. the
     * complete package name, a dot, and then the class name) of an
     * implementation of Astyanax's RetryBackoffStrategy interface. This string
     * may be followed by a sequence of integers, separated from the full
     * classname and from each other by commas; in this case, the integers are
     * cast to native Java ints and passed to the class constructor as
     * arguments. Here's an example setting that would instantiate an Astyanax
     * FixedRetryBackoffStrategy with an delay interval of 1s and suspend time
     * of 5s:
     * <p/>
     * <code>
     * com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy,1000,5000
     * </code>
     * <p/>
     * If null, then Astyanax uses its default strategy, which is an
     * ExponentialRetryBackoffStrategy instance. The instance parameters take
     * Astyanax's built-in default values, which can be overridden via the
     * following config keys:
     * <ul>
     * <li>{@link #RETRY_DELAY_SLICE}</li>
     * <li>{@link #RETRY_MAX_DELAY_SLICE}</li>
     * <li>{@link #RETRY_SUSPEND_WINDOW}</li>
     * </ul>
     * <p/>
     * In Astyanax, RetryPolicy and RetryBackoffStrategy sound and look similar
     * but are used for distinct purposes. RetryPolicy is for retrying failed
     * operations. RetryBackoffStrategy is for retrying attempts to talk to
     * uncommunicative hosts. This config option controls RetryBackoffStrategy.
     */
    public static final ConfigOption<String> RETRY_BACKOFF_STRATEGY = new ConfigOption<String>(STORAGE_NS, "retry-backoff-strategy",
            "Astyanax's retry backoff strategy with configuration parameters",
            ConfigOption.Type.MASKABLE, "com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy,1000,5000");
//    public static final String RETRY_BACKOFF_STRATEGY_DEFAULT = "com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy,1000,5000";
//    //public static final String RETRY_BACKOFF_STRATEGY_DEFAULT = null;
//    public static final String RETRY_BACKOFF_STRATEGY_KEY = "retry-backoff-strategy";

    /**
     * Controls the retryDelaySlice parameter on Astyanax
     * ConnectionPoolConfigurationImpl objects, which is in turn used by
     * ExponentialRetryBackoffStrategy. See the code for
     * {@link ConnectionPoolConfigurationImpl},
     * {@link ExponentialRetryBackoffStrategy}, and the javadoc for
     * {@link #RETRY_BACKOFF_STRATEGY} for more information.
     * <p/>
     * This parameter is not meaningful for and has no effect on
     * FixedRetryBackoffStrategy.
     */
    public static final ConfigOption<Integer> RETRY_DELAY_SLICE = new ConfigOption<Integer>(STORAGE_NS, "retry-delay-slice",
            "Astyanax's connection pool \"retryDelaySlice\" parameter",
            ConfigOption.Type.MASKABLE, ConnectionPoolConfigurationImpl.DEFAULT_RETRY_DELAY_SLICE);
//    public static final String RETRY_DELAY_SLICE = "retry-delay-slice";

    /**
     * Controls the retryMaxDelaySlice parameter on Astyanax
     * ConnectionPoolConfigurationImpl objects, which is in turn used by
     * ExponentialRetryBackoffStrategy. See the code for
     * {@link ConnectionPoolConfigurationImpl},
     * {@link ExponentialRetryBackoffStrategy}, and the javadoc for
     * {@link #RETRY_BACKOFF_STRATEGY} for more information.
     * <p/>
     * This parameter is not meaningful for and has no effect on
     * FixedRetryBackoffStrategy.
     */
    public static final ConfigOption<Integer> RETRY_MAX_DELAY_SLICE = new ConfigOption<Integer>(STORAGE_NS, "retry-max-delay-slice",
            "Astyanax's connection pool \"retryMaxDelaySlice\" parameter",
            ConfigOption.Type.MASKABLE, ConnectionPoolConfigurationImpl.DEFAULT_RETRY_MAX_DELAY_SLICE);
//    public static final String RETRY_MAX_DELAY_SLICE_KEY = "retry-max-delay-slice";

    /**
     * Controls the retrySuspendWindow parameter on Astyanax
     * ConnectionPoolConfigurationImpl objects, which is in turn used by
     * ExponentialRetryBackoffStrategy. See the code for
     * {@link ConnectionPoolConfigurationImpl},
     * {@link ExponentialRetryBackoffStrategy}, and the javadoc for
     * {@link #RETRY_BACKOFF_STRATEGY} for more information.
     * <p/>
     * This parameter is not meaningful for and has no effect on
     * FixedRetryBackoffStrategy.
     */
    public static final ConfigOption<Integer> RETRY_SUSPEND_WINDOW = new ConfigOption<Integer>(STORAGE_NS, "retry-suspend-window",
            "Astyanax's connection pool \"retryMaxDelaySlice\" parameter",
            ConfigOption.Type.MASKABLE, ConnectionPoolConfigurationImpl.DEFAULT_RETRY_SUSPEND_WINDOW);
//    public static final String RETRY_SUSPEND_WINDOW_KEY = "retry-suspend-window";

    private final String clusterName;

    private final AstyanaxContext<Keyspace> keyspaceContext;
    private final AstyanaxContext<Cluster> clusterContext;

    private final RetryPolicy retryPolicy;

    private final int retryDelaySlice;
    private final int retryMaxDelaySlice;
    private final int retrySuspendWindow;
    private final RetryBackoffStrategy retryBackoffStrategy;

    private final Map<String, AstyanaxOrderedKeyColumnValueStore> openStores;

    public AstyanaxStoreManager(Configuration config) throws StorageException {
        super(config);


        // Check if we have non-default thrift frame size or max message size set and warn users
        // because there is nothing we can do in Astyanax to apply those, warning is good enough here
        // otherwise it would make bad user experience if we don't warn at all or crash on this.
        if (config.has(CASSANDRA_THRIFT_FRAME_SIZE))
            log.warn("Couldn't set custom Thrift Frame Size property, use 'cassandrathrift' instead.");

        this.clusterName = config.get(CLUSTER_NAME);

        retryDelaySlice = config.get(RETRY_DELAY_SLICE);
        retryMaxDelaySlice = config.get(RETRY_MAX_DELAY_SLICE);
        retrySuspendWindow = config.get(RETRY_SUSPEND_WINDOW);
        retryBackoffStrategy = getRetryBackoffStrategy(config.get(RETRY_BACKOFF_STRATEGY));
        retryPolicy = getRetryPolicy(config.get(RETRY_POLICY));

        final int maxConnsPerHost = config.get(MAX_CONNECTIONS_PER_HOST);

        final int maxClusterConnsPerHost = config.get(MAX_CLUSTER_CONNECTIONS_PER_HOST);

        this.clusterContext = createCluster(getContextBuilder(config, maxClusterConnsPerHost, "Cluster"));

        ensureKeyspaceExists(clusterContext.getClient());

        this.keyspaceContext = getContextBuilder(config, maxConnsPerHost, "Keyspace").buildKeyspace(ThriftFamilyFactory.getInstance());
        this.keyspaceContext.start();

        openStores = new HashMap<String, AstyanaxOrderedKeyColumnValueStore>(8);
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE; // TODO
    }

    @Override
    @SuppressWarnings("unchecked")
    public IPartitioner<? extends Token<?>> getCassandraPartitioner() throws StorageException {
        Cluster cl = clusterContext.getClient();
        try {
            return FBUtilities.newPartitioner(cl.describePartitioner());
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public String toString() {
        return "astyanax" + super.toString();
    }

    @Override
    public void close() {
        // Shutdown the Astyanax contexts
        openStores.clear();
        keyspaceContext.shutdown();
        clusterContext.shutdown();
    }

    @Override
    public synchronized AstyanaxOrderedKeyColumnValueStore openDatabase(String name) throws StorageException {
        if (openStores.containsKey(name)) return openStores.get(name);
        else {
            ensureColumnFamilyExists(name);
            AstyanaxOrderedKeyColumnValueStore store = new AstyanaxOrderedKeyColumnValueStore(name, keyspaceContext.getClient(), this, retryPolicy);
            openStores.put(name, store);
            return store;
        }
    }

    public AstyanaxRecipeLocker openLocker(String cfName) throws StorageException {

        ColumnFamily<ByteBuffer, String> cf = new ColumnFamily<ByteBuffer, String>(
                cfName,
                ByteBufferSerializer.get(),
                StringSerializer.get());

        return new AstyanaxRecipeLocker.Builder(keyspaceContext.getClient(), cf).build();
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> batch, StoreTransaction txh) throws StorageException {
        MutationBatch m = keyspaceContext.getClient().prepareMutationBatch()
                .setConsistencyLevel(getTx(txh).getWriteConsistencyLevel().getAstyanax())
                .withRetryPolicy(retryPolicy.duplicate());

        final Timestamp timestamp = getTimestamp(txh);


        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> batchentry : batch.entrySet()) {
            String storeName = batchentry.getKey();
            Preconditions.checkArgument(openStores.containsKey(storeName), "Store cannot be found: " + storeName);

            ColumnFamily<ByteBuffer, ByteBuffer> columnFamily = openStores.get(storeName).getColumnFamily();

            Map<StaticBuffer, KCVMutation> mutations = batchentry.getValue();
            for (Map.Entry<StaticBuffer, KCVMutation> ent : mutations.entrySet()) {
                // The CLMs for additions and deletions are separated because
                // Astyanax's operation timestamp cannot be set on a per-delete
                // or per-addition basis.
                KCVMutation titanMutation = ent.getValue();
                ByteBuffer key = ent.getKey().asByteBuffer();

                if (titanMutation.hasDeletions()) {
                    ColumnListMutation<ByteBuffer> dels = m.withRow(columnFamily, key);
                    dels.setTimestamp(timestamp.deletionTime);

                    for (StaticBuffer b : titanMutation.getDeletions())
                        dels.deleteColumn(b.as(StaticBuffer.BB_FACTORY));
                }

                if (titanMutation.hasAdditions()) {
                    ColumnListMutation<ByteBuffer> upds = m.withRow(columnFamily, key);
                    upds.setTimestamp(timestamp.additionTime);

                    for (Entry e : titanMutation.getAdditions())
                        upds.putColumn(e.getColumnAs(StaticBuffer.BB_FACTORY), e.getValueAs(StaticBuffer.BB_FACTORY));
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
    public void clearStorage() throws StorageException {
        try {
            Cluster cluster = clusterContext.getClient();

            Keyspace ks = cluster.getKeyspace(keySpaceName);

            // Not a big deal if Keyspace doesn't not exist (dropped manually by user or tests).
            // This is called on per test setup basis to make sure that previous test cleaned
            // everything up, so first invocation would always fail as Keyspace doesn't yet exist.
            if (ks == null)
                return;

            for (ColumnFamilyDefinition cf : cluster.describeKeyspace(keySpaceName).getColumnFamilyList()) {
                ks.truncateColumnFamily(new ColumnFamily<Object, Object>(cf.getName(), null, null));
            }
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }
    }

    private void ensureColumnFamilyExists(String name) throws StorageException {
        ensureColumnFamilyExists(name, "org.apache.cassandra.db.marshal.BytesType");
    }

    private void ensureColumnFamilyExists(String name, String comparator) throws StorageException {
        Cluster cl = clusterContext.getClient();
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
                                .setComparatorType(comparator);

                ImmutableMap.Builder<String, String> compressionOptions = new ImmutableMap.Builder<String, String>();

                if (compressionEnabled) {
                    compressionOptions.put("sstable_compression", compressionClass)
                            .put("chunk_length_kb", Integer.toString(compressionChunkSizeKB));
                }

                cl.addColumnFamily(cfDef.setCompressionOptions(compressionOptions.build()));
            }
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }
    }

    private static AstyanaxContext<Cluster> createCluster(AstyanaxContext.Builder cb) {
        AstyanaxContext<Cluster> clusterCtx = cb.buildCluster(ThriftFamilyFactory.getInstance());
        clusterCtx.start();

        return clusterCtx;
    }

    private AstyanaxContext.Builder getContextBuilder(Configuration config, int maxConnsPerHost, String usedFor) {

        final ConnectionPoolType poolType = ConnectionPoolType.valueOf(config.get(CONNECTION_POOL_TYPE));

        final NodeDiscoveryType discType = NodeDiscoveryType.valueOf(config.get(NODE_DISCOVERY_TYPE));

        final int maxConnections = config.get(MAX_CONNECTIONS);

        final int maxOperationsPerConnection = config.get(MAX_OPERATIONS_PER_CONNECTION);


        ConnectionPoolConfigurationImpl cpool =
                new ConnectionPoolConfigurationImpl(usedFor + "TitanConnectionPool")
                        .setPort(port)
                        .setMaxOperationsPerConnection(maxOperationsPerConnection)
                        .setMaxConnsPerHost(maxConnsPerHost)
                        .setRetryDelaySlice(retryDelaySlice)
                        .setRetryMaxDelaySlice(retryMaxDelaySlice)
                        .setRetrySuspendWindow(retrySuspendWindow)
                        .setSocketTimeout(connectionTimeout)
                        .setConnectTimeout(connectionTimeout)
                        .setSeeds(StringUtils.join(hostnames, ","));

        if (null != retryBackoffStrategy) {
            cpool.setRetryBackoffStrategy(retryBackoffStrategy);
            log.debug("Custom RetryBackoffStrategy {}", cpool.getRetryBackoffStrategy());
        } else {
            log.debug("Default RetryBackoffStrategy {}", cpool.getRetryBackoffStrategy());
        }

        AstyanaxConfigurationImpl aconf =
                new AstyanaxConfigurationImpl()
                        .setConnectionPoolType(poolType)
                        .setDiscoveryType(discType)
                        .setTargetCassandraVersion("1.2");

        if (0 < maxConnections) {
            cpool.setMaxConns(maxConnections);
        }

        if (hasAuthentication()) {
            cpool.setAuthenticationCredentials(new SimpleAuthenticationCredentials(username, password));
        }

        return new AstyanaxContext.Builder()
                        .forCluster(clusterName)
                        .forKeyspace(keySpaceName)
                        .withAstyanaxConfiguration(aconf)
                        .withConnectionPoolConfiguration(cpool)
                        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());
    }

    private void ensureKeyspaceExists(Cluster cl) throws StorageException {
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

            Map<String, String> stratops = ImmutableMap.of(
                "replication_factor", String.valueOf(replicationFactor));

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

    private static RetryBackoffStrategy getRetryBackoffStrategy(String desc) throws PermanentStorageException {

        if (null == desc)
            return null;

        String[] tokens = desc.split(",");
        String policyClassName = tokens[0];
        int argCount = tokens.length - 1;
        Object[] args = new Object[argCount];
        for (int i = 1; i < tokens.length; i++) {
            args[i - 1] = Integer.valueOf(tokens[i]);
        }

        try {
            RetryBackoffStrategy rbs = instantiate(policyClassName, args, desc);
            log.debug("Instantiated RetryBackoffStrategy object {} from config string \"{}\"", rbs, desc);
            return rbs;
        } catch (Exception e) {
            throw new PermanentStorageException("Failed to instantiate Astyanax RetryBackoffStrategy implementation", e);
        }
    }

    private static RetryPolicy getRetryPolicy(String serializedRetryPolicy) throws StorageException {
        String[] tokens = serializedRetryPolicy.split(",");
        String policyClassName = tokens[0];
        int argCount = tokens.length - 1;
        Object[] args = new Object[argCount];
        for (int i = 1; i < tokens.length; i++) {
            args[i - 1] = Integer.valueOf(tokens[i]);
        }

        try {
            RetryPolicy rp = instantiate(policyClassName, args, serializedRetryPolicy);
            log.debug("Instantiated RetryPolicy object {} from config string \"{}\"", rp, serializedRetryPolicy);
            return rp;
        } catch (Exception e) {
            throw new PermanentStorageException("Failed to instantiate Astyanax Retry Policy class", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <V> V instantiate(String policyClassName,
                                     Object[] args, String raw) throws Exception {

        Class<?> policyClass = Class.forName(policyClassName);

        for (Constructor<?> con : policyClass.getConstructors()) {
            Class<?>[] parameterClasses = con.getParameterTypes();
            if (args.length == parameterClasses.length) {
                boolean allInts = true;
                for (Class<?> pc : parameterClasses) {
                    if (!pc.equals(int.class)) {
                        allInts = false;
                        break;
                    }
                }

                if (!allInts) {
                    break;
                }

                log.debug("About to instantiate class {} with {} arguments", con.toString(), args.length);

                return (V) con.newInstance(args);
            }
        }

        throw new Exception("Failed to identify a class matching the Astyanax Retry Policy config string \"" + raw + "\"");
    }

    @Override
    public Map<String, String> getCompressionOptions(String cf) throws StorageException {
        try {
            Keyspace k = keyspaceContext.getClient();

            KeyspaceDefinition kdef = k.describeKeyspace();

            if (null == kdef) {
                throw new PermanentStorageException("Keyspace " + kdef + " is undefined");
            }

            ColumnFamilyDefinition cfdef = kdef.getColumnFamily(cf);

            if (null == cfdef) {
                throw new PermanentStorageException("Column family " + cf + " is undefined");
            }

            return cfdef.getCompressionOptions();
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }
    }
}


