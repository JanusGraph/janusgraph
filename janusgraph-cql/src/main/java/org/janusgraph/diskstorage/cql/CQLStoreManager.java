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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.vavr.Tuple;
import io.vavr.collection.Array;
import io.vavr.collection.HashMap;
import io.vavr.collection.Iterator;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData.Container;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.builder.CQLMutateManyFunctionBuilder;
import org.janusgraph.diskstorage.cql.builder.CQLMutateManyFunctionWrapper;
import org.janusgraph.diskstorage.cql.builder.CQLProgrammaticConfigurationLoaderBuilder;
import org.janusgraph.diskstorage.cql.builder.CQLSessionBuilder;
import org.janusgraph.diskstorage.cql.builder.CQLStoreFeaturesBuilder;
import org.janusgraph.diskstorage.cql.builder.CQLStoreFeaturesWrapper;
import org.janusgraph.diskstorage.cql.function.mutate.CQLMutateManyFunction;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.hadoop.CqlHadoopStoreManager;
import org.janusgraph.util.datastructures.ExceptionWrapper;
import org.janusgraph.util.stats.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.truncate;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropKeyspace;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_FACTOR;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_OPTIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REPLICATION_STRATEGY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_JMX_ENABLED;
import static org.janusgraph.util.system.ExecuteUtil.executeWithCatching;
import static org.janusgraph.util.system.ExecuteUtil.gracefulExecutorServiceShutdown;
import static org.janusgraph.util.system.ExecuteUtil.throwIfException;

/**
 * This class creates see {@link CQLKeyColumnValueStore CQLKeyColumnValueStores} and handles Cassandra-backed allocation of vertex IDs for JanusGraph (when so
 * configured).
 */
public class CQLStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CQLStoreManager.class);

    public static final String CONSISTENCY_LOCAL_QUORUM = "LOCAL_QUORUM";
    public static final String CONSISTENCY_QUORUM = "QUORUM";

    private static final int DEFAULT_PORT = 9042;

    protected static final CQLSessionBuilder DEFAULT_CQL_SESSION_BUILDER = new CQLSessionBuilder();
    protected static final CQLProgrammaticConfigurationLoaderBuilder DEFAULT_PROGRAMMATIC_CONFIGURATION_LOADER_BUILDER = new CQLProgrammaticConfigurationLoaderBuilder();

    protected static final CQLMutateManyFunctionBuilder DEFAULT_MUTATE_MANY_FUNCTION_BUILDER = new CQLMutateManyFunctionBuilder();
    protected static final CQLStoreFeaturesBuilder DEFAULT_STORE_FEATURES_BUILDER = new CQLStoreFeaturesBuilder();

    private final String keyspace;

    final ExecutorService executorService;
    private final long threadPoolShutdownMaxWaitTime;
    private final CQLMutateManyFunction executeManyFunction;

    private CqlSession session;
    private final StoreFeatures storeFeatures;
    private final Map<String, CQLKeyColumnValueStore> openStores;
    private final Deployment deployment;

    /**
     * Constructor for the {@link CQLStoreManager} given a JanusGraph {@link Configuration}.
     * @param configuration Graph configuration
     * @throws BackendException throws {@link PermanentBackendException} in case CQL connection cannot be initialized or
     * CQLStoreManager cannot be initialized
     */
    public CQLStoreManager(final Configuration configuration) throws BackendException {
        this(configuration, DEFAULT_MUTATE_MANY_FUNCTION_BUILDER, DEFAULT_STORE_FEATURES_BUILDER, DEFAULT_CQL_SESSION_BUILDER, DEFAULT_PROGRAMMATIC_CONFIGURATION_LOADER_BUILDER);
    }

    /**
     * Constructor for the {@link CQLStoreManager} given a JanusGraph {@link Configuration}.
     * @param configuration Graph configuration
     * @param mutateManyFunctionBuilder Builder for mutate many function with or without executor service
     * @param storeFeaturesBuilder Builder for store features function with {@link DistributedStoreManager.Deployment}
     * @param sessionBuilder Builder for {@link CqlSession}
     * @param baseConfigurationLoaderBuilder Builder for {@link CqlSession} main configuration. It's not guaranteed to be used if it's disabled or if other configuration types are provided with higher priority.
     * @throws BackendException throws {@link PermanentBackendException} in case CQL connection cannot be initialized or
     * CQLStoreManager cannot be initialized
     */
    public CQLStoreManager(final Configuration configuration, final CQLMutateManyFunctionBuilder mutateManyFunctionBuilder,
                           final CQLStoreFeaturesBuilder storeFeaturesBuilder, CQLSessionBuilder sessionBuilder,
                           CQLProgrammaticConfigurationLoaderBuilder baseConfigurationLoaderBuilder) throws BackendException {
        super(configuration, DEFAULT_PORT);
        this.keyspace = determineKeyspaceName(configuration);
        this.openStores = new ConcurrentHashMap<>();
        this.session = sessionBuilder.build(getStorageConfig(), hostnames, port, connectionTimeoutMS, baseConfigurationLoaderBuilder);

        try{

            this.threadPoolShutdownMaxWaitTime = configuration.get(EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME);

            initializeJmxMetrics();
            initializeKeyspace();

            CQLMutateManyFunctionWrapper mutateManyFunctionWrapper = mutateManyFunctionBuilder
                .build(session, configuration, times, assignTimestamp, openStores, this::sleepAfterWrite);
            this.executorService = mutateManyFunctionWrapper.getExecutorService();
            this.executeManyFunction = mutateManyFunctionWrapper.getMutateManyFunction();

            CQLStoreFeaturesWrapper storeFeaturesWrapper = storeFeaturesBuilder.build(session, configuration, hostnames);
            deployment = storeFeaturesWrapper.getDeployment();
            storeFeatures = storeFeaturesWrapper.getStoreFeatures();

        } catch (Throwable throwable){
            close();
            throw new PermanentBackendException("Couldn't initialize CQLStoreManager", throwable);
        }
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

    Optional<ExecutorService> getExecutorService() {
        return Optional.ofNullable(executorService);
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
    Integer getGcGraceSeconds(final String name) throws BackendException {
        TableMetadata tableMetadata = getTableMetadata(name);
        Object gcGraceSeconds = tableMetadata.getOptions().get(CqlIdentifier.fromCql("gc_grace_seconds"));
        return (Integer) gcGraceSeconds;
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
    public void close() throws BackendException {
        try {
            ExceptionWrapper exceptionWrapper = new ExceptionWrapper();
            executeWithCatching(this::clearJmxMetrics, exceptionWrapper);
            executeWithCatching(session::close, exceptionWrapper);
            throwIfException(exceptionWrapper);
        } finally {
            gracefulExecutorServiceShutdown(executorService, threadPoolShutdownMaxWaitTime);
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
        executeManyFunction.mutateMany(mutations, txh);
    }

    public static String determineKeyspaceName(Configuration config) {
        if ((!config.has(KEYSPACE) && (config.has(GRAPH_NAME)))) return config.get(GRAPH_NAME);
        return config.get(KEYSPACE);
    }

    @Override
    public Object getHadoopManager() {
        return new CqlHadoopStoreManager(this.session);
    }

}
