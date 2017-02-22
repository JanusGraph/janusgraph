// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.cassandra.astyanax;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.RetryBackoffStrategy;
import com.netflix.astyanax.connectionpool.SSLConnectionContext;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.ExponentialRetryBackoffStrategy;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.janusgraph.diskstorage.cassandra.CassandraTransaction.getTx;

@PreInitializeConfigOptions
public class AstyanaxStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(AstyanaxStoreManager.class);

    //################### ASTYANAX SPECIFIC CONFIGURATION OPTIONS ######################

    public static final ConfigNamespace ASTYANAX_NS =
            new ConfigNamespace(CASSANDRA_NS, "astyanax", "Astyanax-specific Cassandra options");

    /**
     * Default name for the Cassandra cluster
     * <p/>
     */
    public static final ConfigOption<String> CLUSTER_NAME =
            new ConfigOption<String>(ASTYANAX_NS, "cluster-name",
            "Default name for the Cassandra cluster",
            ConfigOption.Type.MASKABLE, "JanusGraph Cluster");

    /**
     * Maximum pooled connections per host.
     * <p/>
     */
    public static final ConfigOption<Integer> MAX_CONNECTIONS_PER_HOST =
            new ConfigOption<Integer>(ASTYANAX_NS, "max-connections-per-host",
            "Maximum pooled connections per host",
            ConfigOption.Type.MASKABLE, 32);

    /**
     * Maximum open connections allowed in the pool (counting all hosts).
     * <p/>
     */
    public static final ConfigOption<Integer> MAX_CONNECTIONS =
            new ConfigOption<Integer>(ASTYANAX_NS, "max-connections",
            "Maximum open connections allowed in the pool (counting all hosts)",
            ConfigOption.Type.MASKABLE, -1);

    /**
     * Maximum number of operations allowed per connection before the connection is closed.
     * <p/>
     */
    public static final ConfigOption<Integer> MAX_OPERATIONS_PER_CONNECTION =
            new ConfigOption<Integer>(ASTYANAX_NS, "max-operations-per-connection",
            "Maximum number of operations allowed per connection before the connection is closed",
            ConfigOption.Type.MASKABLE, 100 * 1000);

    /**
     * Maximum pooled "cluster" connections per host.
     * <p/>
     * These connections are mostly idle and only used for DDL operations
     * (like creating keyspaces).  JanusGraph doesn't need many of these connections
     * in ordinary operation.
     */
    public static final ConfigOption<Integer> MAX_CLUSTER_CONNECTIONS_PER_HOST =
            new ConfigOption<Integer>(ASTYANAX_NS, "max-cluster-connections-per-host",
            "Maximum pooled \"cluster\" connections per host",
            ConfigOption.Type.MASKABLE, 3);

    /**
     * How Astyanax discovers Cassandra cluster nodes. This must be one of the
     * values of the Astyanax NodeDiscoveryType enum.
     * <p/>
     */
    public static final ConfigOption<String> NODE_DISCOVERY_TYPE =
            new ConfigOption<String>(ASTYANAX_NS, "node-discovery-type",
            "How Astyanax discovers Cassandra cluster nodes",
            ConfigOption.Type.MASKABLE, "RING_DESCRIBE");

    /**
     * Astyanax specific host supplier useful only when discovery type set to DISCOVERY_SERVICE or TOKEN_AWARE.
     * Excepts fully qualified class name which extends google.common.base.Supplier<List<Host>>.
     */
    public static final ConfigOption<String> HOST_SUPPLIER =
            new ConfigOption<String>(ASTYANAX_NS, "host-supplier",
            "Host supplier to use when discovery type is set to DISCOVERY_SERVICE or TOKEN_AWARE",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * Astyanax's connection pooler implementation. This must be one of the
     * values of the Astyanax ConnectionPoolType enum.
     * <p/>
     */
    public static final ConfigOption<String> CONNECTION_POOL_TYPE =
            new ConfigOption<String>(ASTYANAX_NS, "connection-pool-type",
            "Astyanax's connection pooler implementation",
            ConfigOption.Type.MASKABLE, "TOKEN_AWARE");

    /**
     * In Astyanax, RetryPolicy and RetryBackoffStrategy sound and look similar
     * but are used for distinct purposes. RetryPolicy is for retrying failed
     * operations. RetryBackoffStrategy is for retrying attempts to talk to
     * uncommunicative hosts. This config option controls RetryPolicy.
     */
    public static final ConfigOption<String> RETRY_POLICY =
            new ConfigOption<String>(ASTYANAX_NS, "retry-policy",
            "Astyanax's retry policy implementation with configuration parameters",
            ConfigOption.Type.MASKABLE, "com.netflix.astyanax.retry.BoundedExponentialBackoff,100,25000,8");

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
    public static final ConfigOption<String> RETRY_BACKOFF_STRATEGY =
            new ConfigOption<String>(ASTYANAX_NS, "retry-backoff-strategy",
            "Astyanax's retry backoff strategy with configuration parameters",
            ConfigOption.Type.MASKABLE, "com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy,1000,5000");

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
    public static final ConfigOption<Integer> RETRY_DELAY_SLICE =
            new ConfigOption<Integer>(ASTYANAX_NS, "retry-delay-slice",
            "Astyanax's connection pool \"retryDelaySlice\" parameter",
            ConfigOption.Type.MASKABLE, ConnectionPoolConfigurationImpl.DEFAULT_RETRY_DELAY_SLICE);
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
    public static final ConfigOption<Integer> RETRY_MAX_DELAY_SLICE =
            new ConfigOption<Integer>(ASTYANAX_NS, "retry-max-delay-slice",
            "Astyanax's connection pool \"retryMaxDelaySlice\" parameter",
            ConfigOption.Type.MASKABLE, ConnectionPoolConfigurationImpl.DEFAULT_RETRY_MAX_DELAY_SLICE);

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
    public static final ConfigOption<Integer> RETRY_SUSPEND_WINDOW =
            new ConfigOption<Integer>(ASTYANAX_NS, "retry-suspend-window",
            "Astyanax's connection pool \"retryMaxDelaySlice\" parameter",
            ConfigOption.Type.MASKABLE, ConnectionPoolConfigurationImpl.DEFAULT_RETRY_SUSPEND_WINDOW);

    /**
     * Controls the frame size of thrift sockets created by Astyanax.
     */
    public static final ConfigOption<Integer> THRIFT_FRAME_SIZE =
            new ConfigOption<Integer>(ASTYANAX_NS, "frame-size",
            "The thrift frame size in mega bytes", ConfigOption.Type.MASKABLE, 15);

    public static final ConfigOption<String> LOCAL_DATACENTER =
            new ConfigOption<String>(ASTYANAX_NS, "local-datacenter",
            "The name of the local or closest Cassandra datacenter.  When set and not whitespace, " +
            "this value will be passed into ConnectionPoolConfigurationImpl.setLocalDatacenter. " +
            "When unset or set to whitespace, setLocalDatacenter will not be invoked.",
            /* It's between either LOCAL or MASKABLE.  MASKABLE could be useful for cases where
               all the JanusGraph instances are closest to the same Cassandra DC. */
            ConfigOption.Type.MASKABLE, String.class);

    private final String clusterName;

    private final AstyanaxContext<Keyspace> keyspaceContext;
    private final AstyanaxContext<Cluster> clusterContext;

    private final RetryPolicy retryPolicy;

    private final int retryDelaySlice;
    private final int retryMaxDelaySlice;
    private final int retrySuspendWindow;
    private final RetryBackoffStrategy retryBackoffStrategy;

    private final String localDatacenter;

    private final Map<String, AstyanaxKeyColumnValueStore> openStores;

    public AstyanaxStoreManager(Configuration config) throws BackendException {
        super(config);

        this.clusterName = config.get(CLUSTER_NAME);

        retryDelaySlice = config.get(RETRY_DELAY_SLICE);
        retryMaxDelaySlice = config.get(RETRY_MAX_DELAY_SLICE);
        retrySuspendWindow = config.get(RETRY_SUSPEND_WINDOW);
        retryBackoffStrategy = getRetryBackoffStrategy(config.get(RETRY_BACKOFF_STRATEGY));
        retryPolicy = getRetryPolicy(config.get(RETRY_POLICY));

        localDatacenter = config.has(LOCAL_DATACENTER) ?
                config.get(LOCAL_DATACENTER) : "";

        final int maxConnsPerHost = config.get(MAX_CONNECTIONS_PER_HOST);

        final int maxClusterConnsPerHost = config.get(MAX_CLUSTER_CONNECTIONS_PER_HOST);

        this.clusterContext = createCluster(getContextBuilder(config, maxClusterConnsPerHost, "Cluster"));

        ensureKeyspaceExists(clusterContext.getClient());

        this.keyspaceContext = getContextBuilder(config, maxConnsPerHost, "Keyspace").buildKeyspace(ThriftFamilyFactory.getInstance());
        this.keyspaceContext.start();

        openStores = new HashMap<String, AstyanaxKeyColumnValueStore>(8);
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE; // TODO
    }

    @Override
    public IPartitioner getCassandraPartitioner() throws BackendException {
        Cluster cl = clusterContext.getClient();
        try {
            return FBUtilities.newPartitioner(cl.describePartitioner());
        } catch (ConnectionException e) {
            throw new TemporaryBackendException(e);
        } catch (ConfigurationException e) {
            throw new PermanentBackendException(e);
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
    public synchronized AstyanaxKeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        if (openStores.containsKey(name)) return openStores.get(name);
        else {
            ensureColumnFamilyExists(name);
            AstyanaxKeyColumnValueStore store = new AstyanaxKeyColumnValueStore(name, keyspaceContext.getClient(), this, retryPolicy);
            openStores.put(name, store);
            return store;
        }
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> batch, StoreTransaction txh) throws BackendException {
        MutationBatch m = keyspaceContext.getClient().prepareMutationBatch().withAtomicBatch(atomicBatch)
                .setConsistencyLevel(getTx(txh).getWriteConsistencyLevel().getAstyanax())
                .withRetryPolicy(retryPolicy.duplicate());

        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> batchentry : batch.entrySet()) {
            String storeName = batchentry.getKey();
            Preconditions.checkArgument(openStores.containsKey(storeName), "Store cannot be found: " + storeName);

            ColumnFamily<ByteBuffer, ByteBuffer> columnFamily = openStores.get(storeName).getColumnFamily();

            Map<StaticBuffer, KCVMutation> mutations = batchentry.getValue();
            for (Map.Entry<StaticBuffer, KCVMutation> ent : mutations.entrySet()) {
                // The CLMs for additions and deletions are separated because
                // Astyanax's operation timestamp cannot be set on a per-delete
                // or per-addition basis.
                KCVMutation janusgraphMutation = ent.getValue();
                ByteBuffer key = ent.getKey().asByteBuffer();

                if (janusgraphMutation.hasDeletions()) {
                    ColumnListMutation<ByteBuffer> dels = m.withRow(columnFamily, key);
                    dels.setTimestamp(commitTime.getDeletionTime(times));

                    for (StaticBuffer b : janusgraphMutation.getDeletions())
                        dels.deleteColumn(b.as(StaticBuffer.BB_FACTORY));
                }

                if (janusgraphMutation.hasAdditions()) {
                    ColumnListMutation<ByteBuffer> upds = m.withRow(columnFamily, key);
                    upds.setTimestamp(commitTime.getAdditionTime(times));

                    for (Entry e : janusgraphMutation.getAdditions()) {
                        Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);

                        if (null != ttl && ttl > 0) {
                            upds.putColumn(e.getColumnAs(StaticBuffer.BB_FACTORY), e.getValueAs(StaticBuffer.BB_FACTORY), ttl);
                        } else {
                            upds.putColumn(e.getColumnAs(StaticBuffer.BB_FACTORY), e.getValueAs(StaticBuffer.BB_FACTORY));
                        }
                    }
                }
            }
        }

        try {
            m.execute();
        } catch (ConnectionException e) {
            throw new TemporaryBackendException(e);
        }

        sleepAfterWrite(txh, commitTime);
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearStorage() throws BackendException {
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
            throw new PermanentBackendException(e);
        }
    }

    private void ensureColumnFamilyExists(String name) throws BackendException {
        ensureColumnFamilyExists(name, "org.apache.cassandra.db.marshal.BytesType");
    }

    private void ensureColumnFamilyExists(String name, String comparator) throws BackendException {
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

                if (storageConfig.has(COMPACTION_STRATEGY)) {
                    cfDef.setCompactionStrategy(storageConfig.get(COMPACTION_STRATEGY));
                }

                if (!compactionOptions.isEmpty()) {
                    cfDef.setCompactionStrategyOptions(compactionOptions);
                }

                if (compressionEnabled) {
                    compressionOptions.put("sstable_compression", compressionClass)
                            .put("chunk_length_kb", Integer.toString(compressionChunkSizeKB));
                }

                cl.addColumnFamily(cfDef.setCompressionOptions(compressionOptions.build()));
            }
        } catch (ConnectionException e) {
            throw new TemporaryBackendException(e);
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

        final int connectionTimeout = (int) connectionTimeoutMS.toMillis();

        ConnectionPoolConfigurationImpl cpool =
                new ConnectionPoolConfigurationImpl(usedFor + "JanusGraphConnectionPool")
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

        if (StringUtils.isNotBlank(localDatacenter)) {
            cpool.setLocalDatacenter(localDatacenter);
            log.debug("Set local datacenter: {}", cpool.getLocalDatacenter());
        }

        AstyanaxConfigurationImpl aconf =
                new AstyanaxConfigurationImpl()
                        .setConnectionPoolType(poolType)
                        .setDiscoveryType(discType)
                        .setTargetCassandraVersion("1.2")
                        .setMaxThriftSize(thriftFrameSizeBytes);

        if (0 < maxConnections) {
            cpool.setMaxConns(maxConnections);
        }

        if (hasAuthentication()) {
            cpool.setAuthenticationCredentials(new SimpleAuthenticationCredentials(username, password));
        }

        if (config.get(SSL_ENABLED)) {
            cpool.setSSLConnectionContext(new SSLConnectionContext(config.get(SSL_TRUSTSTORE_LOCATION), config.get(SSL_TRUSTSTORE_PASSWORD)));
        }

        AstyanaxContext.Builder ctxBuilder = new AstyanaxContext.Builder();

        // Standard context builder options
        ctxBuilder
            .forCluster(clusterName)
            .forKeyspace(keySpaceName)
            .withAstyanaxConfiguration(aconf)
            .withConnectionPoolConfiguration(cpool)
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

        // Conditional context builder option: host supplier
        if (config.has(HOST_SUPPLIER)) {
            String hostSupplier = config.get(HOST_SUPPLIER);
            Supplier<List<Host>> supplier = null;
            if (hostSupplier != null) {
                try {
                    supplier = (Supplier<List<Host>>) Class.forName(hostSupplier).newInstance();
                    ctxBuilder.withHostSupplier(supplier);
                } catch (Exception e) {
                    log.warn("Problem with host supplier class " + hostSupplier + ", going to use default.", e);
                }
            }
        }

        return ctxBuilder;
    }

    private void ensureKeyspaceExists(Cluster cl) throws BackendException {
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
            ksDef = cl.makeKeyspaceDefinition()
                    .setName(keySpaceName)
                    .setStrategyClass(storageConfig.get(REPLICATION_STRATEGY))
                    .setStrategyOptions(strategyOptions);
            cl.addKeyspace(ksDef);

            log.debug("Created keyspace {}", keySpaceName);
        } catch (ConnectionException e) {
            log.debug("Failed to create keyspace {}", keySpaceName);
            throw new TemporaryBackendException(e);
        }
    }

    private static RetryBackoffStrategy getRetryBackoffStrategy(String desc) throws PermanentBackendException {
        if (null == desc)
            return null;

        String[] tokens = desc.split(",");
        String policyClassName = tokens[0];
        int argCount = tokens.length - 1;
        Integer[] args = new Integer[argCount];

        for (int i = 1; i < tokens.length; i++) {
            args[i - 1] = Integer.valueOf(tokens[i]);
        }

        try {
            RetryBackoffStrategy rbs = instantiate(policyClassName, args, desc);
            log.debug("Instantiated RetryBackoffStrategy object {} from config string \"{}\"", rbs, desc);
            return rbs;
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to instantiate Astyanax RetryBackoffStrategy implementation", e);
        }
    }

    private static RetryPolicy getRetryPolicy(String serializedRetryPolicy) throws BackendException {
        String[] tokens = serializedRetryPolicy.split(",");
        String policyClassName = tokens[0];
        int argCount = tokens.length - 1;
        Integer[] args = new Integer[argCount];
        for (int i = 1; i < tokens.length; i++) {
            args[i - 1] = Integer.valueOf(tokens[i]);
        }

        try {
            RetryPolicy rp = instantiate(policyClassName, args, serializedRetryPolicy);
            log.debug("Instantiated RetryPolicy object {} from config string \"{}\"", rp, serializedRetryPolicy);
            return rp;
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to instantiate Astyanax Retry Policy class", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <V> V instantiate(String policyClassName, Integer[] args, String raw) throws Exception {
        for (Constructor<?> con : Class.forName(policyClassName).getConstructors()) {
            Class<?>[] parameterTypes = con.getParameterTypes();

            // match constructor by number of arguments first
            if (args.length != parameterTypes.length)
                continue;

            // check if the constructor parameter types are compatible with argument types (which are integer)
            // note that we allow long.class arguments too because integer is cast to long by runtime.
            boolean intsOrLongs = true;
            for (Class<?> pc : parameterTypes) {
                if (!pc.equals(int.class) && !pc.equals(long.class)) {
                    intsOrLongs = false;
                    break;
                }
            }

            // we found a constructor with required number of parameters but times didn't match, let's carry on
            if (!intsOrLongs)
                continue;

            if (log.isDebugEnabled())
                log.debug("About to instantiate class {} with {} arguments", con.toString(), args.length);

            return (V) con.newInstance(args);
        }

        throw new Exception("Failed to identify a class matching the Astyanax Retry Policy config string \"" + raw + "\"");
    }

    @Override
    public Map<String, String> getCompressionOptions(String cf) throws BackendException {
        try {
            Keyspace k = keyspaceContext.getClient();

            KeyspaceDefinition kdef = k.describeKeyspace();

            if (null == kdef) {
                throw new PermanentBackendException("Keyspace " + k.getKeyspaceName() + " is undefined");
            }

            ColumnFamilyDefinition cfdef = kdef.getColumnFamily(cf);

            if (null == cfdef) {
                throw new PermanentBackendException("Column family " + cf + " is undefined");
            }

            return cfdef.getCompressionOptions();
        } catch (ConnectionException e) {
            throw new PermanentBackendException(e);
        }
    }
}


