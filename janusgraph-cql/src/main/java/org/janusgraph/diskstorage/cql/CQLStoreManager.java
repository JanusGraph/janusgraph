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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.vavr.Tuple;
import io.vavr.collection.Array;
import io.vavr.collection.HashMap;
import io.vavr.collection.Iterator;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData.Container;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.hadoop.CqlHadoopStoreManager;
import org.janusgraph.util.stats.MetricManager;
import org.janusgraph.util.system.NetworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.truncate;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropKeyspace;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ATOMIC_BATCH_MUTATE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.BATCH_STATEMENT_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.HEARTBEAT_INTERVAL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.HEARTBEAT_TIMEOUT;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_DATACENTER;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.MAX_REQUESTS_PER_CONNECTION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METADATA_SCHEMA_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METADATA_TOKEN_MAP_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_EXPIRE_AFTER;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_MESSAGES_HIGHEST_LATENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_MESSAGES_REFRESH_INTERVAL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_MESSAGES_SIGNIFICANT_DIGITS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_REQUESTS_HIGHEST_LATENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_REQUESTS_REFRESH_INTERVAL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_REQUESTS_SIGNIFICANT_DIGITS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_THROTTLING_HIGHEST_LATENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_THROTTLING_REFRESH_INTERVAL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_THROTTLING_SIGNIFICANT_DIGITS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.NETTY_ADMIN_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.NETTY_IO_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.NETTY_TIMER_TICKS_PER_WHEEL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.NETTY_TIMER_TICK_DURATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.PARTITIONER_NAME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.PROTOCOL_VERSION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REMOTE_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_FACTOR;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_OPTIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_STRATEGY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_ERROR_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_MAX_QUERY_LENGTH;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_MAX_VALUES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_MAX_VALUE_LENGTH;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SHOW_STACK_TRACES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SHOW_VALUES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SLOW_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SLOW_THRESHOLD;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SUCCESS_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_TRACKER_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SESSION_NAME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_CLIENT_AUTHENTICATION_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_HOSTNAME_VALIDATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_KEYSTORE_KEY_PASSWORD;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_KEYSTORE_LOCATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_LOCATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.USE_EXTERNAL_LOCKING;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.EXCEPTION_MAPPER;
import static org.janusgraph.diskstorage.cql.CQLTransaction.getTransaction;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_USERNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.CONNECTION_TIMEOUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_JMX_ENABLED;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_PREFIX;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

/**
 * This class creates see {@link CQLKeyColumnValueStore CQLKeyColumnValueStores} and handles Cassandra-backed allocation of vertex IDs for JanusGraph (when so
 * configured).
 */
public class CQLStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CQLStoreManager.class);

    static final String CONSISTENCY_LOCAL_QUORUM = "LOCAL_QUORUM";
    static final String CONSISTENCY_QUORUM = "QUORUM";

    private static final int DEFAULT_PORT = 9042;

    private final String keyspace;
    private final int batchSize;
    private final boolean atomicBatch;

    final ExecutorService executorService;

    private CqlSession session;
    private final StoreFeatures storeFeatures;
    private final Map<String, CQLKeyColumnValueStore> openStores;
    private final Deployment deployment;

    /**
     * Constructor for the {@link CQLStoreManager} given a JanusGraph {@link Configuration}.
     * @param configuration
     * @throws BackendException
     */
    public CQLStoreManager(final Configuration configuration) throws BackendException {
        super(configuration, DEFAULT_PORT);
        this.keyspace = determineKeyspaceName(configuration);
        this.batchSize = configuration.get(BATCH_STATEMENT_SIZE);
        this.atomicBatch = configuration.get(ATOMIC_BATCH_MUTATE);

        this.executorService = new ThreadPoolExecutor(10,
                100,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("CQLStoreManager[%02d]")
                        .build());

        this.session = initializeSession();
        initializeJmxMetrics();
        initializeKeyspace();

        final Configuration global = buildGraphConfiguration()
                .set(READ_CONSISTENCY, CONSISTENCY_QUORUM)
                .set(WRITE_CONSISTENCY, CONSISTENCY_QUORUM)
                .set(METRICS_PREFIX, METRICS_SYSTEM_PREFIX_DEFAULT);

        final Configuration local = buildGraphConfiguration()
                .set(READ_CONSISTENCY, CONSISTENCY_LOCAL_QUORUM)
                .set(WRITE_CONSISTENCY, CONSISTENCY_LOCAL_QUORUM)
                .set(METRICS_PREFIX, METRICS_SYSTEM_PREFIX_DEFAULT);

        final Boolean onlyUseLocalConsistency = configuration.get(ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS);

        final Boolean useExternalLocking = configuration.get(USE_EXTERNAL_LOCKING);

        final StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

        fb.batchMutation(true).distributed(true);
        fb.timestamps(true).cellTTL(true);
        fb.keyConsistent((onlyUseLocalConsistency ? local : global), local);
        fb.locking(useExternalLocking);
        fb.optimisticLocking(true);
        fb.multiQuery(false);

        String partitioner = null;
        if (configuration.has(PARTITIONER_NAME)) {
            partitioner = getShortPartitionerName(configuration.get(PARTITIONER_NAME));
        }
        if (session.getMetadata().getTokenMap().isPresent()) {
            String retrievedPartitioner = getShortPartitionerName(session.getMetadata().getTokenMap().get().getPartitionerName());
            if (partitioner == null) {
                partitioner = retrievedPartitioner;
            } else if (!partitioner.equals(retrievedPartitioner)) {
                throw new IllegalArgumentException(String.format("Provided partitioner (%s) does not match with server (%s)",
                    partitioner, retrievedPartitioner));
            }
        } else if (partitioner == null) {
            throw new IllegalArgumentException(String.format("Partitioner name not provided and cannot retrieve it from " +
                "server, please check %s and %s options", PARTITIONER_NAME.getName(), METADATA_TOKEN_MAP_ENABLED.getName()));
        }
        switch (partitioner) {
            case "DefaultPartitioner": // Amazon managed KeySpace uses com.amazonaws.cassandra.DefaultPartitioner
                fb.timestamps(false).cellTTL(false);
            case "RandomPartitioner":
            case "Murmur3Partitioner": {
                fb.keyOrdered(false).orderedScan(false).unorderedScan(true);
                deployment = Deployment.REMOTE;
                break;
            }
            case "ByteOrderedPartitioner": {
                fb.keyOrdered(true).orderedScan(true).unorderedScan(false);
                deployment = (hostnames.length == 1)// mark deployment as local only in case we have byte ordered partitioner and local
                                                    // connection
                        ? (NetworkUtil.isLocalConnection(hostnames[0])) ? Deployment.LOCAL : Deployment.REMOTE
                        : Deployment.REMOTE;
                break;
            }
            default: {
                throw new IllegalArgumentException("Unrecognized partitioner: " + partitioner);
            }
        }
        this.storeFeatures = fb.build();
        this.openStores = new ConcurrentHashMap<>();
    }

    CqlSession initializeSession() throws PermanentBackendException {
        final Configuration configuration = getStorageConfig();

        final List<InetSocketAddress> contactPoints;
        try {
            contactPoints = Array.of(this.hostnames)
                .map(hostName -> hostName.split(":"))
                .map(array -> Tuple.of(array[0], array.length == 2 ? Integer.parseInt(array[1]) : this.port))
                .map(tuple -> new InetSocketAddress(tuple._1, tuple._2))
                .toJavaList();
        } catch (SecurityException | ArrayIndexOutOfBoundsException | NumberFormatException e) {
            throw new PermanentBackendException("Error initialising cluster contact points", e);
        }

        final CqlSessionBuilder builder = CqlSession.builder()
            .addContactPoints(contactPoints)
            .withLocalDatacenter(configuration.get(LOCAL_DATACENTER));

        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder();
        configLoaderBuilder.withString(DefaultDriverOption.SESSION_NAME, configuration.get(SESSION_NAME));
        configLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, configuration.get(CONNECTION_TIMEOUT));

        if (configuration.get(PROTOCOL_VERSION) != 0) {
            configLoaderBuilder.withInt(DefaultDriverOption.PROTOCOL_VERSION, configuration.get(PROTOCOL_VERSION));
        }

        if (configuration.has(AUTH_USERNAME) && configuration.has(AUTH_PASSWORD)) {
            configLoaderBuilder
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, configuration.get(AUTH_USERNAME))
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, configuration.get(AUTH_PASSWORD));
        }

        if (configuration.get(SSL_ENABLED)) {
            configLoaderBuilder
                .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
                .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, configuration.get(SSL_TRUSTSTORE_LOCATION))
                .withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, configuration.get(SSL_TRUSTSTORE_PASSWORD))
                .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, configuration.get(SSL_HOSTNAME_VALIDATION));

            if(configuration.get(SSL_CLIENT_AUTHENTICATION_ENABLED)) {
                configLoaderBuilder
                    .withString(DefaultDriverOption.SSL_KEYSTORE_PATH, configuration.get(SSL_KEYSTORE_LOCATION))
                    .withString(DefaultDriverOption.SSL_KEYSTORE_PASSWORD, configuration.get(SSL_KEYSTORE_KEY_PASSWORD));
            }
        }

        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, configuration.get(LOCAL_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, configuration.get(REMOTE_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, configuration.get(MAX_REQUESTS_PER_CONNECTION));

        if(configuration.has(HEARTBEAT_INTERVAL)){
            configLoaderBuilder.withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL,
                Duration.ofMillis(configuration.get(HEARTBEAT_INTERVAL)));
        }

        if(configuration.has(HEARTBEAT_TIMEOUT)){
            configLoaderBuilder.withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT,
                Duration.ofMillis(configuration.get(HEARTBEAT_TIMEOUT)));
        }

        if (configuration.has(METADATA_SCHEMA_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, configuration.get(METADATA_SCHEMA_ENABLED));
        }

        if (configuration.has(METADATA_TOKEN_MAP_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, configuration.get(METADATA_TOKEN_MAP_ENABLED));
        }

        // Keep to 0 for the time being: https://groups.google.com/a/lists.datastax.com/forum/#!topic/java-driver-user/Bc0gQuOVVL0
        // Ideally we want to batch all tables initialisations to happen together when opening a new keyspace
        configLoaderBuilder.withInt(DefaultDriverOption.METADATA_SCHEMA_WINDOW, 0);

        configureCqlNetty(configuration, configLoaderBuilder);

        if (configuration.get(BASIC_METRICS)) {
            configureMetrics(configuration, configLoaderBuilder);
        }

        configureRequestTracker(configuration, configLoaderBuilder);

        builder.withConfigLoader(configLoaderBuilder.build());

        return builder.build();
    }

    private void initializeJmxMetrics() {
        final Configuration configuration = getStorageConfig();
        if (configuration.get(METRICS_JMX_ENABLED) && configuration.get(BASIC_METRICS) && session.getMetrics().isPresent()) {
            MetricManager.INSTANCE.getRegistry().registerAll(session.getMetrics().get().getRegistry());
        }
    }

    private void clearJmxMetrics() {
        final Configuration configuration = getStorageConfig();
        if (configuration.get(METRICS_JMX_ENABLED) && configuration.get(BASIC_METRICS) && session.getMetrics().isPresent()) {
            session.getMetrics().get().getRegistry().getNames().forEach(metricName -> MetricManager.INSTANCE.getRegistry().remove(metricName));
        }
    }

    void initializeKeyspace(){
        // if the keyspace already exists, just return
        if (this.session.getMetadata().getKeyspace(this.keyspace).isPresent()) {
            return;
        }

        final Configuration configuration = getStorageConfig();

        // Setting replication strategy based on value reading from the configuration: either "SimpleStrategy" or "NetworkTopologyStrategy"
        final Map<String, Object> replication = Match(configuration.get(REPLICATION_STRATEGY)).of(
            Case($("SimpleStrategy"), strategy -> HashMap.<String, Object> of("class", strategy, "replication_factor", configuration.get(REPLICATION_FACTOR))),
            Case($("NetworkTopologyStrategy"),
                strategy -> HashMap.<String, Object> of("class", strategy)
                    .merge(Array.of(configuration.get(REPLICATION_OPTIONS))
                        .grouped(2)
                        .toMap(array -> Tuple.of(array.get(0), Integer.parseInt(array.get(1)))))))
            .toJavaMap();

        session.execute(createKeyspace(this.keyspace)
            .ifNotExists()
            .withReplicationOptions(replication)
            .build());
    }

    ExecutorService getExecutorService() {
        return this.executorService;
    }

    CqlSession getSession() {
        return this.session;
    }

    String getKeyspaceName() {
        return this.keyspace;
    }

    @VisibleForTesting
    Map<String, String> getCompressionOptions(final String name) throws BackendException {
        TableMetadata tableMetadata = getTableMetadata(name);
        Object compressionOptions = tableMetadata.getOptions().get(CqlIdentifier.fromCql("compression"));
        return (Map<String, String>) compressionOptions;
    }

    @VisibleForTesting
    String getSpeculativeRetry(final String name) throws BackendException {
        TableMetadata tableMetadata = getTableMetadata(name);
        Object res = tableMetadata.getOptions().get(CqlIdentifier.fromCql("speculative_retry"));
        return (String) res;
    }

    @VisibleForTesting
    TableMetadata getTableMetadata(final String name) throws BackendException {
        final KeyspaceMetadata keyspaceMetadata = (this.session.getMetadata().getKeyspace(this.keyspace))
            .orElseThrow(() -> new PermanentBackendException(String.format("Unknown keyspace '%s'", this.keyspace)));
        return keyspaceMetadata.getTable(name)
            .orElseThrow(() -> new PermanentBackendException(String.format("Unknown table '%s'", name)));
    }

    @Override
    public void close() {
        try {
            clearJmxMetrics();
            this.session.close();
        } finally {
            this.executorService.shutdownNow();
        }
    }

    @Override
    public String getName() {
        return String.format("%s.%s", getClass().getSimpleName(), this.keyspace);
    }

    @Override
    public Deployment getDeployment() {
        return this.deployment;
    }

    @Override
    public StoreFeatures getFeatures() {
        return this.storeFeatures;
    }

    @Override
    public KeyColumnValueStore openDatabase(final String name, final Container metaData) throws BackendException {
        return this.openStores.computeIfAbsent(name, n -> new CQLKeyColumnValueStore(this, n, getStorageConfig(), () -> this.openStores.remove(n)));
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws BackendException {
        return new CQLTransaction(config);
    }

    @Override
    public void clearStorage() throws BackendException {
        if (this.storageConfig.get(DROP_ON_CLEAR)) {
            this.session.execute(dropKeyspace(this.keyspace).build());
        } else if (this.exists()) {
            final Future<Seq<AsyncResultSet>> result = Future.sequence(
                Iterator.ofAll(this.session.getMetadata().getKeyspace(this.keyspace).get().getTables().values())
                    .map(table -> Future.fromJavaFuture(this.session.executeAsync(truncate(this.keyspace, table.getName().toString()).build())
                        .toCompletableFuture())));
            result.await();
        } else {
            LOGGER.info("Keyspace {} does not exist in the cluster", this.keyspace);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        return session.getMetadata().getKeyspace(this.keyspace).isPresent();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mutateMany(final Map<String, Map<StaticBuffer, KCVMutation>> mutations, final StoreTransaction txh) throws BackendException {
        if (this.atomicBatch) {
            mutateManyLogged(mutations, txh);
        } else {
            mutateManyUnlogged(mutations, txh);
        }
    }

    // Use a single logged batch
    private void mutateManyLogged(final Map<String, Map<StaticBuffer, KCVMutation>> mutations, final StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = assignTimestamp ? new MaskedTimestamp(txh) : null;

        BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.LOGGED);
        builder.setConsistencyLevel(getTransaction(txh).getWriteConsistencyLevel());

        Iterator.ofAll(mutations.entrySet()).flatMap(tableNameAndMutations -> {
            final String tableName = tableNameAndMutations.getKey();
            final Map<StaticBuffer, KCVMutation> tableMutations = tableNameAndMutations.getValue();

            final CQLKeyColumnValueStore columnValueStore = Option.of(this.openStores.get(tableName))
                    .getOrElseThrow(() -> new IllegalStateException("Store cannot be found: " + tableName));
            return Iterator.ofAll(tableMutations.entrySet()).flatMap(keyAndMutations -> {
                final StaticBuffer key = keyAndMutations.getKey();
                final KCVMutation keyMutations = keyAndMutations.getValue();

                Iterator<BatchableStatement<BoundStatement>> deletions;
                Iterator<BatchableStatement<BoundStatement>> additions;
                if (commitTime != null) {
                    deletions = Iterator.of(commitTime.getDeletionTime(this.times))
                        .flatMap(deleteTime -> Iterator.ofAll(keyMutations.getDeletions()).map(deletion -> columnValueStore.deleteColumn(key, deletion, deleteTime)));
                    additions = Iterator.of(commitTime.getAdditionTime(this.times))
                        .flatMap(addTime -> Iterator.ofAll(keyMutations.getAdditions()).map(addition -> columnValueStore.insertColumn(key, addition, addTime)));
                } else {
                    deletions = Iterator.ofAll(keyMutations.getDeletions()).map(deletion -> columnValueStore.deleteColumn(key, deletion));
                    additions = Iterator.ofAll(keyMutations.getAdditions()).map(addition -> columnValueStore.insertColumn(key, addition));
                }

                return Iterator.concat(deletions, additions);
            });
        }).forEach(builder::addStatement);

        final Future<AsyncResultSet> result = Future.fromJavaFuture(this.executorService, this.session.executeAsync(builder.build()).toCompletableFuture());

        result.await();
        if (result.isFailure()) {
            throw EXCEPTION_MAPPER.apply(result.getCause().get());
        }
        if (commitTime != null) {
            sleepAfterWrite(txh, commitTime);
        }
    }

    // Create an async un-logged batch per partition key
    private void mutateManyUnlogged(final Map<String, Map<StaticBuffer, KCVMutation>> mutations, final StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = assignTimestamp ? new MaskedTimestamp(txh) : null;

        final Future<Seq<AsyncResultSet>> result = Future.sequence(this.executorService, Iterator.ofAll(mutations.entrySet()).flatMap(tableNameAndMutations -> {
            final String tableName = tableNameAndMutations.getKey();
            final Map<StaticBuffer, KCVMutation> tableMutations = tableNameAndMutations.getValue();

            final CQLKeyColumnValueStore columnValueStore = Option.of(this.openStores.get(tableName))
                    .getOrElseThrow(() -> new IllegalStateException("Store cannot be found: " + tableName));
            return Iterator.ofAll(tableMutations.entrySet()).flatMap(keyAndMutations -> {
                final StaticBuffer key = keyAndMutations.getKey();
                final KCVMutation keyMutations = keyAndMutations.getValue();

                Iterator<BatchableStatement<BoundStatement>> deletions;
                Iterator<BatchableStatement<BoundStatement>> additions;
                if (commitTime != null) {
                    deletions = Iterator.of(commitTime.getDeletionTime(this.times))
                        .flatMap(deleteTime -> Iterator.ofAll(keyMutations.getDeletions()).map(deletion -> columnValueStore.deleteColumn(key, deletion, deleteTime)));
                    additions = Iterator.of(commitTime.getAdditionTime(this.times))
                        .flatMap(addTime -> Iterator.ofAll(keyMutations.getAdditions()).map(addition -> columnValueStore.insertColumn(key, addition, addTime)));
                } else {
                    deletions = Iterator.ofAll(keyMutations.getDeletions()).map(deletion -> columnValueStore.deleteColumn(key, deletion));
                    additions = Iterator.ofAll(keyMutations.getAdditions()).map(addition -> columnValueStore.insertColumn(key, addition));
                }

                return Iterator.concat(deletions, additions)
                        .grouped(this.batchSize)
                        .map(group -> Future.fromJavaFuture(this.executorService,
                                this.session.executeAsync(
                                    BatchStatement.newInstance(DefaultBatchType.UNLOGGED)
                                                .addAll(group)
                                                .setConsistencyLevel(getTransaction(txh).getWriteConsistencyLevel()))
                                    .toCompletableFuture()));
            });
        }));

        result.await();
        if (result.isFailure()) {
            throw EXCEPTION_MAPPER.apply(result.getCause().get());
        }
        if (commitTime != null) {
            sleepAfterWrite(txh, commitTime);
        }
    }

    private String determineKeyspaceName(Configuration config) {
        if ((!config.has(KEYSPACE) && (config.has(GRAPH_NAME)))) return config.get(GRAPH_NAME);
        return config.get(KEYSPACE);
    }

    private void configureCqlNetty(Configuration configuration, ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder){
        // The following sets the size of Netty ThreadPool executor used by Cassandra driver:
        // https://docs.datastax.com/en/developer/java-driver/4.8/manual/core/async/#threading-model
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_IO_SIZE, configuration.get(NETTY_IO_SIZE));
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_ADMIN_SIZE, configuration.get(NETTY_ADMIN_SIZE));

        if(configuration.has(NETTY_TIMER_TICK_DURATION)){
            configLoaderBuilder.withDuration(DefaultDriverOption.NETTY_TIMER_TICK_DURATION,
                Duration.ofMillis(configuration.get(NETTY_TIMER_TICK_DURATION)));
        }
        if(configuration.has(NETTY_TIMER_TICKS_PER_WHEEL)){
            configLoaderBuilder.withInt(DefaultDriverOption.NETTY_TIMER_TICKS_PER_WHEEL, configuration.get(NETTY_TIMER_TICKS_PER_WHEEL));
        }

        // Keep the following values to 0 so that when we close the session we don't have to wait for the
        // so called "quiet period", setting this to a different value will slow down Graph.close()
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD, 0);
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, 0);
    }

    private void configureMetrics(Configuration configuration, ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder){
        if(configuration.has(METRICS_SESSION_ENABLED)){
            configLoaderBuilder.withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED,
                Arrays.asList(configuration.get(METRICS_SESSION_ENABLED)));
            if(configuration.has(METRICS_SESSION_REQUESTS_HIGHEST_LATENCY)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST,
                    Duration.ofMillis(configuration.get(METRICS_SESSION_REQUESTS_HIGHEST_LATENCY)));
            }
            if(configuration.has(METRICS_SESSION_REQUESTS_SIGNIFICANT_DIGITS)){
                configLoaderBuilder.withInt(DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS,
                    configuration.get(METRICS_SESSION_REQUESTS_SIGNIFICANT_DIGITS));
            }
            if(configuration.has(METRICS_SESSION_REQUESTS_REFRESH_INTERVAL)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL,
                    Duration.ofMillis(configuration.get(METRICS_SESSION_REQUESTS_REFRESH_INTERVAL)));
            }
            if(configuration.has(METRICS_SESSION_THROTTLING_HIGHEST_LATENCY)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST,
                    Duration.ofMillis(configuration.get(METRICS_SESSION_THROTTLING_HIGHEST_LATENCY)));
            }
            if(configuration.has(METRICS_SESSION_THROTTLING_SIGNIFICANT_DIGITS)){
                configLoaderBuilder.withInt(DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS,
                    configuration.get(METRICS_SESSION_THROTTLING_SIGNIFICANT_DIGITS));
            }
            if(configuration.has(METRICS_SESSION_THROTTLING_REFRESH_INTERVAL)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL,
                    Duration.ofMillis(configuration.get(METRICS_SESSION_THROTTLING_REFRESH_INTERVAL)));
            }
        }
        if(configuration.has(METRICS_NODE_ENABLED)){
            configLoaderBuilder.withStringList(DefaultDriverOption.METRICS_NODE_ENABLED,
                Arrays.asList(configuration.get(METRICS_NODE_ENABLED)));
            if(configuration.has(METRICS_NODE_MESSAGES_HIGHEST_LATENCY)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST,
                    Duration.ofMillis(configuration.get(METRICS_NODE_MESSAGES_HIGHEST_LATENCY)));
            }
            if(configuration.has(METRICS_NODE_MESSAGES_SIGNIFICANT_DIGITS)){
                configLoaderBuilder.withInt(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS,
                    configuration.get(METRICS_NODE_MESSAGES_SIGNIFICANT_DIGITS));
            }
            if(configuration.has(METRICS_NODE_MESSAGES_REFRESH_INTERVAL)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL,
                    Duration.ofMillis(configuration.get(METRICS_NODE_MESSAGES_REFRESH_INTERVAL)));
            }
            if(configuration.has(METRICS_NODE_EXPIRE_AFTER)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER,
                    Duration.ofMillis(configuration.get(METRICS_NODE_EXPIRE_AFTER)));
            }
        }
    }

    private void configureRequestTracker(Configuration configuration, ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder){
        if (configuration.has(REQUEST_TRACKER_CLASS)) {
            configLoaderBuilder.withString(DefaultDriverOption.REQUEST_TRACKER_CLASS, configuration.get(REQUEST_TRACKER_CLASS));
        }
        if (configuration.has(REQUEST_LOGGER_SUCCESS_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED,
                configuration.get(REQUEST_LOGGER_SUCCESS_ENABLED));
        }
        if (configuration.has(REQUEST_LOGGER_SLOW_THRESHOLD)) {
            configLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD,
                Duration.ofMillis(configuration.get(REQUEST_LOGGER_SLOW_THRESHOLD)));
        }
        if (configuration.has(REQUEST_LOGGER_SLOW_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED,
                configuration.get(REQUEST_LOGGER_SLOW_ENABLED));
        }
        if (configuration.has(REQUEST_LOGGER_ERROR_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED,
                configuration.get(REQUEST_LOGGER_ERROR_ENABLED));
        }
        if (configuration.has(REQUEST_LOGGER_MAX_QUERY_LENGTH)) {
            configLoaderBuilder.withInt(DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
                configuration.get(REQUEST_LOGGER_MAX_QUERY_LENGTH));
        }
        if (configuration.has(REQUEST_LOGGER_SHOW_VALUES)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_VALUES,
                configuration.get(REQUEST_LOGGER_SHOW_VALUES));
        }
        if (configuration.has(REQUEST_LOGGER_MAX_VALUE_LENGTH)) {
            configLoaderBuilder.withInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH,
                configuration.get(REQUEST_LOGGER_MAX_VALUE_LENGTH));
        }
        if (configuration.has(REQUEST_LOGGER_MAX_VALUES)) {
            configLoaderBuilder.withInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES,
                configuration.get(REQUEST_LOGGER_MAX_VALUES));
        }
        if (configuration.has(REQUEST_LOGGER_SHOW_STACK_TRACES)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES,
                configuration.get(REQUEST_LOGGER_SHOW_STACK_TRACES));
        }
    }

    @Override
    public Object getHadoopManager() {
        return new CqlHadoopStoreManager(this.session);
    }

    private String getShortPartitionerName(String partitioner) {
        if (partitioner == null) return null;
        return partitioner.substring(partitioner.lastIndexOf('.') + 1);
    }

}
