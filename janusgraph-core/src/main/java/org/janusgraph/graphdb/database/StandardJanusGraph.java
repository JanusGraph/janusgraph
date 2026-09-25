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

package org.janusgraph.graphdb.database;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeysQueriesGroup;
import org.janusgraph.diskstorage.keycolumnvalue.MultiKeysQueryGroups;
import org.janusgraph.diskstorage.keycolumnvalue.MultiQueriesByKeysGroupsContext;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.cache.CacheInvalidationService;
import org.janusgraph.graphdb.database.cache.KCVSCacheInvalidationService;
import org.janusgraph.graphdb.database.cache.SchemaCache;
import org.janusgraph.graphdb.database.idassigner.VertexIDAssigner;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphHasStepStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphLocalQueryOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphUnusedMultiQueryRemovalStrategy;
import org.janusgraph.graphdb.util.MultiSliceQueriesGroupingUtil;
import org.janusgraph.util.IDUtils;
import org.janusgraph.graphdb.database.index.IndexInfoRetriever;
import org.janusgraph.graphdb.database.index.IndexUpdate;
import org.janusgraph.graphdb.database.util.IndexAppliesToFunction;
import org.janusgraph.graphdb.database.util.IndexRecordUtil;
import org.janusgraph.graphdb.database.log.LogTxStatus;
import org.janusgraph.graphdb.database.log.TransactionLogHeader;
import org.janusgraph.graphdb.database.management.ManagementLogger;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.InternalVertexLabel;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.query.index.IndexSelectionStrategy;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.tinkerpop.JanusGraphBlueprintsGraph;
import org.janusgraph.graphdb.tinkerpop.JanusGraphFeatures;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexFilterOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexHasIdOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexHasUniquePropertyOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.AdjacentVertexIsOptimizerStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphIoRegistrationStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphMixedIndexAggStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphMixedIndexCountStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphMultiQueryStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.JanusGraphStepStrategy;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.system.BaseRelationType;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.graphdb.util.ExceptionFactory;
import org.janusgraph.util.system.IOUtils;
import org.janusgraph.util.system.TXUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.script.Bindings;
import javax.script.ScriptException;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_ACK_TIMEOUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_AUTO_CLOSE_STALE_INSTANCES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REGISTRATION_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REPLACE_INSTANCE_IF_EXISTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SCRIPT_EVAL_ENABLED;

public class StandardJanusGraph extends JanusGraphBlueprintsGraph {

    private static final Logger log =
            LoggerFactory.getLogger(StandardJanusGraph.class);

    private static final Logger logForPrepareCommit =
        LoggerFactory.getLogger(StandardJanusGraph.class.getName()+".prepareCommit");

    static {
        TraversalStrategies graphStrategies =
            TraversalStrategies.GlobalCache.getStrategies(Graph.class)
                .clone()
                .addStrategies(AdjacentVertexFilterOptimizerStrategy.instance(),
                               AdjacentVertexHasIdOptimizerStrategy.instance(),
                               AdjacentVertexIsOptimizerStrategy.instance(),
                               AdjacentVertexHasUniquePropertyOptimizerStrategy.instance(),
                               JanusGraphLocalQueryOptimizerStrategy.instance(),
                               JanusGraphHasStepStrategy.instance(),
                               JanusGraphMultiQueryStrategy.instance(),
                               JanusGraphUnusedMultiQueryRemovalStrategy.instance(),
                               JanusGraphMixedIndexAggStrategy.instance(),
                               JanusGraphMixedIndexCountStrategy.instance(),
                               JanusGraphStepStrategy.instance(),
                               JanusGraphIoRegistrationStrategy.instance());

        //Register with cache
        TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraph.class, graphStrategies);
        TraversalStrategies.GlobalCache.registerStrategies(StandardJanusGraphTx.class, graphStrategies);
    }

    private final GraphDatabaseConfiguration config;
    /** Backing index names configured as cdc-only (index.[X].cdc.enabled=true and cdc.synchronous=false):
     *  their mixed-index mutations are skipped on the synchronous commit path and applied asynchronously
     *  by the CDC pipeline (janusgraph-cdc worker) instead. */
    private final Set<String> cdcOnlyBackingIndexes;
    /** The index filter used when generating commit-time index updates for ADDED relations and mutated properties:
     *  standard applicability ({@link IndexRecordUtil#FULL_INDEX_APPLIES_TO_FILTER}) minus the mixed indexes on
     *  cdc-only backends -- their updates are filtered out at GENERATION (not just skipped at write time) so the hot
     *  commit path never pays for serializing entries that would only be discarded. Composite indexes are never
     *  filtered. Reindex/repair jobs and transaction recovery use their own filters and are unaffected -- they still
     *  write cdc-only indexes. */
    private final IndexAppliesToFunction commitIndexAppliesToFilter;
    /** The index filter for DELETED relations whose documents the CDC stream cannot identify (see
     *  {@link #cdcCanIdentifyDeletedRelation}, which selects between this and {@link #commitIndexAppliesToFilter}
     *  per deleted relation). Like {@link #commitIndexAppliesToFilter}, but relation-element (edge / meta-property)
     *  mixed indexes on cdc-only backends are NOT filtered: those document DELETIONS stay synchronous even in
     *  cdc-only mode. The CDC pipeline can always rebuild ADDED/updated documents from the element's current state,
     *  but it cannot identify a deleted relation's document when no change event carries the relation identity --
     *  Cassandra tombstones carry no value bytes (constrained-multiplicity edge ids and meta-property ids live in
     *  the value region), and a whole-row vertex removal ({@code storage.drop-whole-row-on-vertex-removal}) emits
     *  one partition-level delete with no per-edge identity at all (self-loops, unidirected edges, and edges whose
     *  both endpoints are removed in one transaction have no surviving mirror event either). Those identities only
     *  exist HERE, at commit time, so this is where their document removals are issued; a removal is idempotent
     *  under the CDC applier's reindex-from-current-state, so late CDC events for the same relation converge to the
     *  same result. Deleted relations whose id is re-added by the same transaction (updates) are exempt from the
     *  synchronous removal -- their document id stays live and the worker rewrites it from current state, and a
     *  synchronous whole-document delete could otherwise land after that rewrite (e.g. a delayed index write) and
     *  erase the fresh document with no later event to restore it. */
    private final IndexAppliesToFunction commitDeletionIndexAppliesToFilter;
    private final Backend backend;
    private final IDManager idManager;
    private final boolean wholeRowDeletionEnabled;
    private final VertexIDAssigner idAssigner;
    private final TimestampProvider times;
    private final CacheInvalidationService cacheInvalidationService;


    //Serializers
    protected final IndexSerializer indexSerializer;
    protected final EdgeSerializer edgeSerializer;
    protected final Serializer serializer;

    //Caches
    public final SliceQuery vertexExistenceQuery;
    private final RelationQueryCache queryCache;
    private final SchemaCache schemaCache;

    //Log
    private final ManagementLogger managementLogger;

    //Shutdown hook
    private volatile ShutdownThread shutdownHook;

    //Index selection
    private final IndexSelectionStrategy indexSelector;

    //Gremlin Script Engine
    private final GremlinScriptEngine scriptEngine;

    private volatile boolean isOpen;
    private final AtomicLong txCounter;

    private final Set<StandardJanusGraphTx> openTransactions;

    private final String name;

    public StandardJanusGraph(GraphDatabaseConfiguration configuration) {

        this.config = configuration;
        this.cdcOnlyBackingIndexes =
            GraphDatabaseConfiguration.getCdcBackingIndexNames(configuration.getConfiguration(), true);
        this.commitIndexAppliesToFilter = cdcOnlyBackingIndexes.isEmpty()
            ? IndexRecordUtil.FULL_INDEX_APPLIES_TO_FILTER
            : (index, element) -> IndexRecordUtil.indexAppliesTo(index, element) && !isCdcOnlyMixedIndex(index);
        this.commitDeletionIndexAppliesToFilter = cdcOnlyBackingIndexes.isEmpty()
            ? IndexRecordUtil.FULL_INDEX_APPLIES_TO_FILTER
            : (index, element) -> IndexRecordUtil.indexAppliesTo(index, element)
                && (!isCdcOnlyMixedIndex(index) || index.getElement().isRelation());
        if (!cdcOnlyBackingIndexes.isEmpty()) {
            // There is no way to validate from here that a capture pipeline (e.g. Cassandra CDC + Debezium + Kafka +
            // the janusgraph-cdc worker) is actually running, so make the trade-off loud: with cdc-only configured and
            // no pipeline, mixed indexes silently stop being maintained (and the WAL records these transactions as
            // fully successful, so transaction recovery will not repair them either).
            log.warn("Mixed index backend(s) {} are configured cdc-only (index.[X].cdc.enabled=true and "
                + "index.[X].cdc.synchronous=false): synchronous index additions are SKIPPED for them (only "
                + "relation-document deletions that CDC events cannot identify are still written synchronously). "
                + "Ensure the external CDC pipeline and the janusgraph-cdc worker are running, otherwise these "
                + "indexes will not be updated.",
                cdcOnlyBackingIndexes);
        }
        this.backend = configuration.getBackend();

        this.name = configuration.getGraphName();

        this.idAssigner = config.getIDAssigner(backend);
        this.idManager = idAssigner.getIDManager();
        this.wholeRowDeletionEnabled = configuration.getDropWholeRowOnVertexRemoval()
            && backend.getStoreFeatures().hasOptimizedWholeRowDeletion();

        this.cacheInvalidationService = new KCVSCacheInvalidationService(
            backend.getEdgeStoreCache(), backend.getIndexStoreCache(), idManager);

        this.serializer = config.getSerializer();
        this.edgeSerializer = new EdgeSerializer(this.serializer);
        StoreFeatures storeFeatures = backend.getStoreFeatures();
        this.indexSerializer = new IndexSerializer(configuration.getConfiguration(), this.edgeSerializer, this.serializer,
                this.backend.getIndexInformation(), storeFeatures.isDistributed() && storeFeatures.isKeyOrdered());
        this.vertexExistenceQuery = edgeSerializer.getQuery(BaseKey.VertexExists, Direction.OUT, new EdgeSerializer.TypedInterval[0]).setLimit(1);
        this.queryCache = new RelationQueryCache(this.edgeSerializer);
        this.schemaCache = configuration.getTypeCache(typeCacheRetrieval);
        this.times = configuration.getTimestampProvider();
        this.indexSelector = getConfiguration().getIndexSelectionStrategy();

        if (configuration.hasScriptEval()) {
            log.info("Gremlin script evaluation is enabled");
            this.scriptEngine = config.getScriptEngine();
        } else {
            log.info("Gremlin script evaluation is disabled");
            this.scriptEngine = null;
        }

        isOpen = true;
        txCounter = new AtomicLong(0);
        openTransactions = Collections.newSetFromMap(new ConcurrentHashMap<>(100, 0.75f, 1));

        //Register instance and ensure uniqueness
        String uniqueInstanceId = configuration.getUniqueGraphId();
        ModifiableConfiguration globalConfig = getGlobalSystemConfig(backend);
        final boolean instanceExists = globalConfig.has(REGISTRATION_TIME, uniqueInstanceId);
        final boolean replaceExistingInstance = configuration.getConfiguration().get(REPLACE_INSTANCE_IF_EXISTS);
        if (instanceExists && !replaceExistingInstance) {
            throw new JanusGraphException(String.format("A JanusGraph graph with the same instance id [%s] is already open. Might required forced shutdown.", uniqueInstanceId));
        } else if (instanceExists && replaceExistingInstance) {
            log.debug(String.format("Instance [%s] already exists. Opening the graph per " + REPLACE_INSTANCE_IF_EXISTS.getName() + " configuration.", uniqueInstanceId));
        }
        globalConfig.set(REGISTRATION_TIME, times.getTime(), uniqueInstanceId);

        Log managementLog = backend.getSystemMgmtLog();
        Duration ackTimeout = configuration.getConfiguration().get(MANAGEMENT_ACK_TIMEOUT);
        boolean autoCloseStaleInstances = configuration.getConfiguration().get(MANAGEMENT_AUTO_CLOSE_STALE_INSTANCES);
        managementLogger = new ManagementLogger(this, managementLog, schemaCache, this.times, ackTimeout, autoCloseStaleInstances);
        managementLog.registerReader(ReadMarker.fromNow(), managementLogger);

        shutdownHook = new ShutdownThread(this);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        log.debug("Installed shutdown hook {}", shutdownHook, new Throwable("Hook creation trace"));
    }

    public String getGraphName() {
        return this.name;
    }

    @Override
    public Object eval(String gremlinScript, boolean commit) {
        Objects.requireNonNull(scriptEngine, String.format("%s is not enabled", SCRIPT_EVAL_ENABLED.toStringWithoutRoot()));
        JanusGraphTransaction tx = newTransaction();
        try {
            Bindings bindings = scriptEngine.createBindings();
            GraphTraversalSource traversalSource = tx.traversal();
            if (!commit) {
                // this is usually not necessary as we will rollback at the end anyway, but when
                // batch-loading is true, writes might be persisted even before rollback happens,
                // so we should always use ReadOnlyStrategy as a safe guard
                traversalSource = traversalSource.withStrategies(ReadOnlyStrategy.instance());
            }
            bindings.put("g", traversalSource);
            return scriptEngine.eval(gremlinScript, bindings);
        } catch (ScriptException e) {
            throw new JanusGraphException("Could not evaluate given gremlin script: " + gremlinScript, e);
        } finally {
            if (tx.isOpen()) {
                if (commit) {
                    tx.commit();
                } else {
                    tx.rollback();
                }
            } else {
                log.error("Transaction associated with script engine is wrongly closed. This might indicate the script " +
                    "is malicious: {}", gremlinScript);
            }
        }
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isClosed() {
        return !isOpen();
    }

    @Override
    public synchronized void close() throws JanusGraphException {
        try {
            closeInternal();
        } finally {
            removeHook();
        }
    }

    @Override
    public CacheInvalidationService getDBCacheInvalidationService() {
        return cacheInvalidationService;
    }

    private synchronized void closeInternal() {

        if (!isOpen) return;

        Map<JanusGraphTransaction, RuntimeException> txCloseExceptions = new HashMap<>();

        try {
            //Unregister instance
            String uniqueId = null;
            try {
                uniqueId = config.getUniqueGraphId();
                ModifiableConfiguration globalConfig = getGlobalSystemConfig(backend);
                globalConfig.remove(REGISTRATION_TIME, uniqueId);
            } catch (Exception e) {
                log.warn("Unable to remove graph instance uniqueid {}", uniqueId, e);
            }

            /* Assuming a couple of properties about openTransactions:
             * 1. no concurrent modifications during graph shutdown
             * 2. all contained txs are open
             */
            for (StandardJanusGraphTx otx : openTransactions) {
                try {
                    otx.rollback();
                    otx.close();
                } catch (RuntimeException e) {
                    // Catch and store these exceptions, but proceed wit the loop
                    // Any remaining txs on the iterator should get a chance to close before we throw up
                    log.warn("Unable to close transaction {}", otx, e);
                    txCloseExceptions.put(otx, e);
                }
            }

            super.close();

            IOUtils.closeQuietly(idAssigner);
            IOUtils.closeQuietly(backend);
            IOUtils.closeQuietly(queryCache);
            IOUtils.closeQuietly(serializer);
            if(config instanceof Closeable){
                IOUtils.closeQuietly((Closeable)config);
            }
        } finally {
            isOpen = false;
        }

        // Throw an exception if at least one transaction failed to close
        if (1 == txCloseExceptions.size()) {
            // TP3's test suite requires that this be of type ISE
            throw new IllegalStateException("Unable to close transaction",
                    Iterables.getOnlyElement(txCloseExceptions.values()));
        } else if (1 < txCloseExceptions.size()) {
            throw new IllegalStateException(String.format(
                    "Unable to close %s transactions (see warnings in log output for details)",
                    txCloseExceptions.size()));
        }
    }

    private synchronized void removeHook() {
        if (null == shutdownHook)
                return;

        ShutdownThread tmp = shutdownHook;
        shutdownHook = null;
        // Remove shutdown hook to avoid reference retention
        try {
            Runtime.getRuntime().removeShutdownHook(tmp);
            log.debug("Removed shutdown hook {}", tmp);
        } catch (IllegalStateException e) {
            log.warn("Failed to remove shutdown hook", e);
        }
    }


    // ################### Simple Getters #########################

    @Override
    public Features features() {
        return JanusGraphFeatures.getFeatures(this, backend.getStoreFeatures());
    }


    public IndexSerializer getIndexSerializer() {
        return indexSerializer;
    }

    public IndexSelectionStrategy getIndexSelector() {
        return indexSelector;
    }

    public Backend getBackend() {
        return backend;
    }

    public IDManager getIDManager() {
        return idManager;
    }

    public EdgeSerializer getEdgeSerializer() {
        return edgeSerializer;
    }

    public Serializer getDataSerializer() {
        return serializer;
    }

    //TODO: premature optimization, re-evaluate later
//    public RelationQueryCache getQueryCache() {
//        return queryCache;
//    }

    public SchemaCache getSchemaCache() {
        return schemaCache;
    }

    public GraphDatabaseConfiguration getConfiguration() {
        return config;
    }

    @Override
    public JanusGraphManagement openManagement() {
        return new ManagementSystem(this,backend.getGlobalSystemConfig(),backend.getSystemMgmtLog(), managementLogger, schemaCache);
    }

    public Set<? extends JanusGraphTransaction> getOpenTransactions() {
        return new HashSet<>(openTransactions);
    }

    // ################### TRANSACTIONS #########################

    @Override
    public JanusGraphTransaction newTransaction() {
        return buildTransaction().start();
    }

    @Override
    public StandardTransactionBuilder buildTransaction() {
        return new StandardTransactionBuilder(getConfiguration(), this);
    }

    @Override
    public JanusGraphTransaction newThreadBoundTransaction() {
        return buildTransaction().threadBound().start();
    }

    public StandardJanusGraphTx newTransaction(final TransactionConfiguration configuration) {
        if (!isOpen) ExceptionFactory.graphShutdown();
        try {
            StandardJanusGraphTx tx = new StandardJanusGraphTx(this, configuration);
            tx.setBackendTransaction(openBackendTransaction(tx));
            openTransactions.add(tx);
            return tx;
        } catch (BackendException e) {
            throw new JanusGraphException("Could not start new transaction", e);
        }
    }

    private BackendTransaction openBackendTransaction(StandardJanusGraphTx tx) throws BackendException {
        IndexInfoRetriever retriever = indexSerializer.getIndexInfoRetriever(tx);
        return backend.beginTransaction(tx.getConfiguration(), retriever);
    }

    public void closeTransaction(StandardJanusGraphTx tx) {
        openTransactions.remove(tx);
    }

    // ################### READ #########################

    private final SchemaCache.StoreRetrieval typeCacheRetrieval = new SchemaCache.StoreRetrieval() {

        @Override
        public Long retrieveSchemaByName(String typeName) {
            // Get a consistent tx
            Configuration customTxOptions = backend.getStoreFeatures().getKeyConsistentTxConfig();
            StandardJanusGraphTx consistentTx = null;
            try {
                consistentTx = StandardJanusGraph.this.newTransaction(new StandardTransactionBuilder(getConfiguration(),
                        StandardJanusGraph.this, customTxOptions).groupName(GraphDatabaseConfiguration.METRICS_SCHEMA_PREFIX_DEFAULT));
                consistentTx.getTxHandle().disableCache();
                JanusGraphVertex v = Iterables.getOnlyElement(QueryUtil.getVertices(consistentTx, BaseKey.SchemaName, typeName), null);
                return v != null? ((Number) v.id()).longValue(): null;
            } finally {
                TXUtils.rollbackQuietly(consistentTx);
            }
        }

        @Override
        public EntryList retrieveSchemaRelations(final long schemaId, final BaseRelationType type, final Direction dir) {
            SliceQuery query = queryCache.getQuery(type,dir);
            Configuration customTxOptions = backend.getStoreFeatures().getKeyConsistentTxConfig();
            StandardJanusGraphTx consistentTx = null;
            try {
                consistentTx = StandardJanusGraph.this.newTransaction(new StandardTransactionBuilder(getConfiguration(),
                        StandardJanusGraph.this, customTxOptions).groupName(GraphDatabaseConfiguration.METRICS_SCHEMA_PREFIX_DEFAULT));
                consistentTx.getTxHandle().disableCache();
                return edgeQuery(schemaId, query, consistentTx.getTxHandle());
            } finally {
                TXUtils.rollbackQuietly(consistentTx);
            }
        }

    };

    public RecordIterator<Object> getVertexIDs(final BackendTransaction tx) {
        Preconditions.checkArgument(backend.getStoreFeatures().hasOrderedScan() ||
                backend.getStoreFeatures().hasUnorderedScan(),
                "The configured storage backend does not support global graph operations - use Faunus instead");

        final KeyIterator keyIterator;
        if (backend.getStoreFeatures().hasUnorderedScan()) {
            keyIterator = tx.edgeStoreKeys(vertexExistenceQuery);
        } else {
            keyIterator = tx.edgeStoreKeys(new KeyRangeQuery(IDHandler.MIN_KEY, IDHandler.MAX_KEY, vertexExistenceQuery));
        }

        return new RecordIterator<Object>() {

            @Override
            public boolean hasNext() {
                return keyIterator.hasNext();
            }

            @Override
            public Object next() {
                return idManager.getKeyID(keyIterator.next());
            }

            @Override
            public void close() throws IOException {
                keyIterator.close();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Removal not supported");
            }
        };
    }

    public EntryList edgeQuery(Object vid, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(!(vid instanceof Number) || ((Number) vid).longValue() > 0);
        return tx.edgeStoreQuery(new KeySliceQuery(idManager.getKey(vid), query));
    }

    public List<EntryList> edgeMultiQuery(List<Object> vertexIdsAsObjects, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(vertexIdsAsObjects != null && !vertexIdsAsObjects.isEmpty());
        final List<StaticBuffer> vertexIds = new ArrayList<>(vertexIdsAsObjects.size());
        for (Object vertexIdsAsObject : vertexIdsAsObjects) {
            IDUtils.checkId(vertexIdsAsObject);
            vertexIds.add(idManager.getKey(vertexIdsAsObject));
        }
        final Map<StaticBuffer,EntryList> result = tx.edgeStoreMultiQuery(vertexIds, query);
        final List<EntryList> resultList = new ArrayList<>(result.size());
        for (StaticBuffer v : vertexIds) resultList.add(result.get(v));
        return resultList;
    }

    private StaticBuffer[] convertVertexIds(Object[] vertexIds){
        StaticBuffer[] convertedIds = new StaticBuffer[vertexIds.length];
        for(int i=0;i<vertexIds.length;i++){
            IDUtils.checkId(vertexIds[i]);
            convertedIds[i] = idManager.getKey(vertexIds[i]);
        }
        return convertedIds;
    }

    public Map<SliceQuery, Map<Object, EntryList>> edgeMultiQuery(MultiKeysQueryGroups<Object, SliceQuery> groupedMultiSliceQueries, BackendTransaction tx) {
        assert groupedMultiSliceQueries != null && !groupedMultiSliceQueries.getQueryGroups().isEmpty();

        Object[] vertexIds = groupedMultiSliceQueries.getMultiQueryContext().getAllKeysArr();
        StaticBuffer[] vertexKeys = convertVertexIds(vertexIds);
        Map<Object, StaticBuffer> vertexIdToKey = new HashMap<>(vertexKeys.length);
        Map<StaticBuffer, Object> keyToVertexId = new HashMap<>(vertexKeys.length);
        for(int i=0; i<vertexIds.length; i++){
            vertexIdToKey.put(vertexIds[i], vertexKeys[i]);
            keyToVertexId.put(vertexKeys[i], vertexIds[i]);
        }

        List<KeysQueriesGroup<StaticBuffer, SliceQuery>> groupedMultiSliceQueriesWithKeys = new ArrayList<>(groupedMultiSliceQueries.getQueryGroups().size());

        for(KeysQueriesGroup<Object, SliceQuery> queriesToVertexIdsPaid : groupedMultiSliceQueries.getQueryGroups()){
            List<StaticBuffer> keys = new ArrayList<>(queriesToVertexIdsPaid.getKeysGroup().size());
            for (Object vertexIdsAsObject : queriesToVertexIdsPaid.getKeysGroup()) {
                keys.add(vertexIdToKey.get(vertexIdsAsObject));
            }
            groupedMultiSliceQueriesWithKeys.add(new KeysQueriesGroup<>(keys, queriesToVertexIdsPaid.getQueries()));
        }

        MultiSliceQueriesGroupingUtil.replaceCurrentLeafNodeWithUpdatedTypeLeafNodes(groupedMultiSliceQueries.getMultiQueryContext().getAllLeafParents(), vertexIdToKey);
        final Map<SliceQuery, Map<StaticBuffer, EntryList>> resultWithKeys = tx.edgeStoreMultiQuery(
            new MultiKeysQueryGroups<>(groupedMultiSliceQueriesWithKeys, new MultiQueriesByKeysGroupsContext<>(vertexKeys, groupedMultiSliceQueries.getMultiQueryContext())));

        final Map<SliceQuery, Map<Object, EntryList>> resultWithVertexIds = new HashMap<>(resultWithKeys.size());
        resultWithKeys.forEach((sliceQuery, keyToResultMap) -> {
            Map<Object, EntryList> vertexIdToResultMap = new HashMap<>(keyToResultMap.size());
            keyToResultMap.forEach((key, result) -> vertexIdToResultMap.put(keyToVertexId.get(key), result));
            resultWithVertexIds.put(sliceQuery, vertexIdToResultMap);
        });

        return resultWithVertexIds;
    }

    private ModifiableConfiguration getGlobalSystemConfig(Backend backend) {

        return new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
            backend.getGlobalSystemConfig(), BasicConfiguration.Restriction.GLOBAL);
    }

    // ################### WRITE #########################

    public void assignID(InternalRelation relation) {
        idAssigner.assignID(relation);
    }

    public void assignID(InternalVertex vertex, VertexLabel label) {
        idAssigner.assignID(vertex,label);
    }

    public static boolean acquireLock(InternalRelation relation, int pos, boolean acquireLocksConfig) {
        InternalRelationType type = (InternalRelationType)relation.getType();
        return acquireLocksConfig && type.getConsistencyModifier()== ConsistencyModifier.LOCK &&
                ( type.multiplicity().isUnique(EdgeDirection.fromPosition(pos))
                        || pos==0 && type.multiplicity()== Multiplicity.SIMPLE);
    }

    public static boolean acquireLock(CompositeIndexType index, boolean acquireLocksConfig) {
        return acquireLocksConfig && index.getConsistencyModifier()==ConsistencyModifier.LOCK
                && index.getCardinality()!= Cardinality.LIST;
    }

    /**
     * The TTL of a relation (edge or property) is the minimum of:
     * 1) The TTL configured of the relation type (if exists)
     * 2) The TTL configured for the label any of the relation end point vertices (if exists)
     *
     * @param rel relation to determine the TTL for
     * @return TTL
     */
    public static int getTTL(InternalRelation rel) {
        assert rel.isNew();
        InternalRelationType baseType = (InternalRelationType) rel.getType();
        assert baseType.getBaseType()==null;
        int ttl = 0;
        Integer ettl = baseType.getTTL();
        if (ettl>0) ttl = ettl;
        for (int i=0;i<rel.getArity();i++) {
            int vttl = getTTL(rel.getVertex(i));
            if (vttl>0 && (vttl<ttl || ttl<=0)) ttl = vttl;
        }
        return ttl;
    }

    public static int getTTL(InternalVertex v) {
        assert v.hasId();
        if (IDManager.VertexIDType.UnmodifiableVertex.is(v.id())) {
            assert v.isNew() : "Should not be able to add relations to existing static vertices: " + v;
            return ((InternalVertexLabel)v.vertexLabel()).getTTL();
        } else return 0;
    }

    private static class ModificationSummary {

        final boolean hasModifications;
        final boolean has2iModifications;

        private ModificationSummary(boolean hasModifications, boolean has2iModifications) {
            this.hasModifications = hasModifications;
            this.has2iModifications = has2iModifications;
        }
    }

    public ModificationSummary prepareCommit(final Collection<InternalRelation> addedRelations,
                                             final Collection<InternalRelation> deletedRelations,
                                             final Predicate<InternalRelation> filter,
                                             final BackendTransaction mutator,
                                             final StandardJanusGraphTx tx,
                                             final boolean acquireLocks) throws BackendException {

        ListMultimap<Object, InternalRelation> mutations = ArrayListMultimap.create();
        ListMultimap<InternalVertex, InternalRelation> mutatedProperties = ArrayListMultimap.create();
        List<IndexUpdate> indexUpdates = new ArrayList<>();

        long startTimeStamp = System.currentTimeMillis();
        int totalMutations = addedRelations.size() + deletedRelations.size();
        boolean isBigDataSetLoggingEnabled = logForPrepareCommit.isDebugEnabled() && totalMutations >= backend.getBufferSize();
        if (isBigDataSetLoggingEnabled) {
            logForPrepareCommit.debug("0. Prepare commit for mutations count={}", totalMutations);
        }

        if (isBigDataSetLoggingEnabled) {
            logForPrepareCommit.debug("1. Collect deleted edges and their index updates and acquire edge locks");
        }
        // Ids of the relations added in this same transaction: a deleted relation whose id is re-added is an
        // UPDATE (the replacement keeps the relation id unless the type has ConsistencyModifier.FORK), meaning its
        // index document id stays live. Only computed when cdc-only indexes exist -- it feeds the per-relation
        // deletion-filter choice below.
        final Set<Long> replacedRelationIds = cdcOnlyBackingIndexes.isEmpty() || deletedRelations.isEmpty()
            ? Collections.emptySet() : collectRelationIds(addedRelations);
        prepareCommitDeletes(deletedRelations, replacedRelationIds, filter, mutator, tx, acquireLocks, mutations,
            mutatedProperties, indexUpdates);

        if (isBigDataSetLoggingEnabled) {
            logForPrepareCommit.debug("2. Collect added edges and their index updates and acquire edge locks");
        }
        prepareCommitAdditions(addedRelations, filter, mutator, tx, acquireLocks, mutations, mutatedProperties, indexUpdates);

        if (isBigDataSetLoggingEnabled) {
            logForPrepareCommit.debug("3. Collect all index update for vertices");
        }
        prepareCommitVertexIndexUpdates(mutatedProperties, tx, indexUpdates);

        if (isBigDataSetLoggingEnabled) {
            logForPrepareCommit.debug("4. Acquire index locks (deletions first)");
        }
        prepareCommitAcquireIndexLocks(indexUpdates, mutator, acquireLocks);

        if (isBigDataSetLoggingEnabled) {
            logForPrepareCommit.debug("5. Add relation mutations");
        }
        prepareCommitAddRelationMutations(mutations, mutator, tx);

        if (isBigDataSetLoggingEnabled) {
            logForPrepareCommit.debug("6. Add index updates");
        }
        boolean has2iMods = prepareCommitIndexUpdatesAndCheckIfAnyMixedIndexUsed(indexUpdates, mutator);

        if (isBigDataSetLoggingEnabled) {
            long duration = System.currentTimeMillis() - startTimeStamp;
            logForPrepareCommit.debug("7. Prepare commit is done with mutated vertex count={} in duration={}", mutations.size(), duration);
        }
        return new ModificationSummary(!mutations.isEmpty(), has2iMods);
    }

    /**
     * Collect deleted edges and their index updates and acquire edge locks
     */
    private void prepareCommitDeletes(final Collection<InternalRelation> deletedRelations,
                                      final Set<Long> replacedRelationIds,
                                      final Predicate<InternalRelation> filter,
                                      final BackendTransaction mutator,
                                      final StandardJanusGraphTx tx,
                                      final boolean acquireLocks,
                                      final ListMultimap<Object, InternalRelation> mutations,
                                      final ListMultimap<InternalVertex, InternalRelation> mutatedProperties,
                                      final List<IndexUpdate> indexUpdates) throws BackendException {
        for(InternalRelation del : deletedRelations){
            if(!filter.test(del)){
                continue;
            }
            Preconditions.checkArgument(del.isRemoved());
            for (int pos = 0; pos < del.getLen(); pos++) {
                InternalVertex vertex = del.getVertex(pos);
                if (pos == 0 || !del.isLoop()) {
                    if (del.isProperty()) mutatedProperties.put(vertex,del);
                    mutations.put(vertex.id(), del);
                }
                if (acquireLock(del,pos,acquireLocks)) {
                    Entry entry = edgeSerializer.writeRelation(del, pos, tx);
                    mutator.acquireEdgeLock(idManager.getKey(vertex.id()), entry);
                }
            }
            // The widened deletion filter (synchronous cdc-only relation-document removals) is applied only when the
            // CDC stream cannot identify this relation's document. When a surviving endpoint's row keeps an ordinary
            // column tombstone carrying the full edge identity (the "mirror" copy), the CDC worker performs the
            // removal instead and the commit path stays free of redundant index deletes -- for a super-node removal
            // with surviving neighbors that is the difference between zero synchronous index operations and one per
            // incident edge. A deleted relation whose id this same transaction RE-ADDS (an update; the replacement
            // keeps the relation id unless the type is ConsistencyModifier.FORK) must not be deleted synchronously
            // either: its document id stays live, and the worker rewrites that document from current state -- a
            // synchronous whole-document delete could land AFTER the worker's rewrite (e.g. a delayed index write)
            // and erase it with no later event to restore it. Skipping leaves the document briefly stale (ordinary
            // CDC lag) instead of indefinitely missing.
            final IndexAppliesToFunction deletionFilter =
                cdcOnlyBackingIndexes.isEmpty() || cdcCanIdentifyDeletedRelation(del, tx)
                    || replacedRelationIds.contains(del.longId())
                    ? commitIndexAppliesToFilter : commitDeletionIndexAppliesToFilter;
            indexUpdates.addAll(indexSerializer.getIndexUpdates(del, deletionFilter, tx));
        }
    }

    /** The assigned ids of the given relations (used to recognize deleted relations that are re-added -- i.e.
     *  updated -- within the same transaction). */
    private static Set<Long> collectRelationIds(final Collection<InternalRelation> relations) {
        if (relations.isEmpty()) {
            return Collections.emptySet();
        }
        final Set<Long> ids = new HashSet<>(relations.size() * 2);
        for (InternalRelation relation : relations) {
            if (relation.hasId()) {
                ids.add(relation.longId());
            }
        }
        return ids;
    }

    /**
     * Whether the CDC pipeline will be able to identify this deleted relation's index document from the change
     * events this transaction produces: true iff at least one storage copy of the relation is removed via a
     * per-column tombstone whose <em>column</em> carries the full relation identity. That holds only for
     * non-constrained (MULTI-multiplicity) edges with at least one incident vertex that is not itself fully removed
     * in this transaction -- the surviving vertex's row receives an ordinary column tombstone for the edge (its
     * mirror copy), from which the CDC decoder reconstructs the exact {@code RelationIdentifier}. Everything else
     * produces no identifying event and needs the synchronous document removal: vertex properties and
     * constrained-multiplicity edges keep their relation id in the <em>value</em> region (absent from tombstones);
     * unidirected edges store no mirror; and when every copy-holding endpoint is fully removed, its row is either
     * whole-row-deleted (one partition tombstone, no columns) or its column tombstones are indistinguishable noise
     * behind the same removal.
     *
     * <p>Deliberately independent of {@link #wholeRowDeletionEnabled}: with whole-row deletion off, fully-removed
     * vertices do emit per-column tombstones and the synchronous removal is merely redundant (idempotent under the
     * worker's reindex-from-current-state) -- preferred over coupling the index guarantee to a storage feature flag.</p>
     */
    private boolean cdcCanIdentifyDeletedRelation(final InternalRelation del, final StandardJanusGraphTx tx) {
        if (!del.isEdge()) {
            return false;
        }
        final InternalRelationType type = (InternalRelationType) del.getType();
        if (type.multiplicity().isConstrained()) {
            return false;
        }
        for (int pos = 0; pos < del.getArity(); pos++) {
            // Same predicate the mutation writer uses: a storage copy exists at this position iff the type covers
            // its direction (normal edges store a copy at both endpoints; unidirected ones only at the out-vertex).
            if (!type.isUnidirected(Direction.BOTH) && !type.isUnidirected(EdgeDirection.fromPosition(pos))) {
                continue;
            }
            final Object vertexId = del.getVertex(pos).id();
            final Object canonicalId = idManager.isPartitionedVertex(vertexId)
                ? idManager.getCanonicalVertexId(((Number) vertexId).longValue())
                : vertexId;
            if (!tx.isVertexFullyRemoved(canonicalId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Collect added edges and their index updates and acquire edge locks
     */
    private void prepareCommitAdditions(final Collection<InternalRelation> addedRelations,
                                        final Predicate<InternalRelation> filter,
                                        final BackendTransaction mutator,
                                        final StandardJanusGraphTx tx,
                                        final boolean acquireLocks,
                                        final ListMultimap<Object, InternalRelation> mutations,
                                        final ListMultimap<InternalVertex, InternalRelation> mutatedProperties,
                                        final List<IndexUpdate> indexUpdates) throws BackendException {
        for (InternalRelation add : addedRelations) {
            if(!filter.test(add)){
                continue;
            }
            Preconditions.checkArgument(add.isNew());
            for (int pos = 0; pos < add.getLen(); pos++) {
                InternalVertex vertex = add.getVertex(pos);
                if (pos == 0 || !add.isLoop()) {
                    if (add.isProperty()) mutatedProperties.put(vertex,add);
                    mutations.put(vertex.id(), add);
                }
                if (!vertex.isNew() && acquireLock(add,pos,acquireLocks)) {
                    Entry entry = edgeSerializer.writeRelation(add, pos, tx);
                    mutator.acquireEdgeLock(idManager.getKey(vertex.id()), entry.getColumn());
                }
            }
            indexUpdates.addAll(indexSerializer.getIndexUpdates(add, commitIndexAppliesToFilter, tx));
        }
    }

    /**
     * Collect all index update for vertices
     */
    private void prepareCommitVertexIndexUpdates(final ListMultimap<InternalVertex, InternalRelation> mutatedProperties,
                                                 final StandardJanusGraphTx tx,
                                                 final List<IndexUpdate> indexUpdates) {
        indexUpdates.addAll(mutatedProperties.keySet().parallelStream()
            .flatMap(v -> indexSerializer.getIndexUpdates(v, mutatedProperties.get(v), commitIndexAppliesToFilter, tx))
            .collect(Collectors.toList()));
    }

    /**
     * Acquire index locks (deletions first)
     */
    private void prepareCommitAcquireIndexLocks(final List<IndexUpdate> indexUpdates,
                                                final BackendTransaction mutator,
                                                final boolean acquireLocks) throws BackendException {
        for (IndexUpdate update : indexUpdates) {
            if (!update.isCompositeIndex() || !update.isDeletion()) continue;
            CompositeIndexType iIndex = (CompositeIndexType) update.getIndex();
            if (acquireLock(iIndex,acquireLocks)) {
                mutator.acquireIndexLock((StaticBuffer)update.getKey(), (Entry)update.getEntry());
            }
        }
        for (IndexUpdate update : indexUpdates) {
            if (!update.isCompositeIndex() || update.isDeletion()) continue;
            CompositeIndexType iIndex = (CompositeIndexType) update.getIndex();
            if (acquireLock(iIndex,acquireLocks)) {
                mutator.acquireIndexLock((StaticBuffer)update.getKey(), ((Entry)update.getEntry()).getColumn());
            }
        }
    }

    /**
     * Add relation mutations
     */
    private void prepareCommitAddRelationMutations(final ListMultimap<Object, InternalRelation> mutations,
                                                   final BackendTransaction mutator,
                                                   final StandardJanusGraphTx tx) throws BackendException {
        for (Object vertexId : mutations.keySet()) {
            IDUtils.checkId(vertexId);
            final List<InternalRelation> edges = mutations.get(vertexId);

            final Object canonicalId = idManager.isPartitionedVertex(vertexId)
                ? idManager.getCanonicalVertexId(((Number) vertexId).longValue())
                : vertexId;
            final boolean wholeRowDeletion = wholeRowDeletionEnabled && tx.isVertexFullyRemoved(canonicalId);

            // When the whole row is deleted, per-column deletions are skipped and a fully-removed
            // vertex has no additions, so both lists stay empty. Avoid preallocating large backing
            // arrays sized to edges.size() for super-node removals.
            final List<Entry> additions = new ArrayList<>(wholeRowDeletion ? 0 : edges.size());
            final List<Entry> deletions = new ArrayList<>(wholeRowDeletion ? 0 : Math.max(10, edges.size() / 10));

            for (final InternalRelation edge : edges) {
                final InternalRelationType baseType = (InternalRelationType) edge.getType();
                assert baseType.getBaseType()==null;

                for (InternalRelationType type : baseType.getRelationIndexes()) {
                    if (type.getStatus()== SchemaStatus.DISABLED) continue;
                    for (int pos = 0; pos < edge.getArity(); pos++) {
                        if (!type.isUnidirected(Direction.BOTH) && !type.isUnidirected(EdgeDirection.fromPosition(pos)))
                            continue; //Directionality is not covered
                        if (edge.getVertex(pos).id().equals(vertexId)) {
                            if (edge.isRemoved()) {
                                // Skip serializing per-column deletions entirely when a whole-row delete will be issued:
                                // writeRelation() is not called, so removing a super-node avoids serializing every incident
                                // edge (CPU + allocations) only to discard the result. wholeRowDeletion is true only when the
                                // backend advertises the capability, so backends without it always serialize the full
                                // per-column deletions list (no data loss).
                                if (!wholeRowDeletion) {
                                    deletions.add(edgeSerializer.writeRelation(edge, type, pos, tx));
                                }
                            } else {
                                Preconditions.checkArgument(edge.isNew());
                                StaticArrayEntry entry = edgeSerializer.writeRelation(edge, type, pos, tx);
                                int ttl = getTTL(edge);
                                if (ttl > 0) {
                                    entry.setMetaData(EntryMetaData.TTL, ttl);
                                }
                                additions.add(entry);
                            }
                        }
                    }
                }
            }

            StaticBuffer vertexKey = idManager.getKey(vertexId);
            mutator.mutateEdges(vertexKey, additions, deletions, wholeRowDeletion);
        }
    }

    /** Whether this is a mixed index on a cdc-only backend, i.e. one whose synchronous updates are not generated. */
    private boolean isCdcOnlyMixedIndex(IndexType index) {
        return index.isMixedIndex() && cdcOnlyBackingIndexes.contains(((MixedIndexType) index).getBackingIndexName());
    }

    /**
     * Add index updates
     *
     * @return `true` if there was any mixed index update
     */
    private boolean prepareCommitIndexUpdatesAndCheckIfAnyMixedIndexUsed(final List<IndexUpdate> indexUpdates,
                                                                         final BackendTransaction mutator) throws BackendException {
        boolean has2iMods = false;
        for (IndexUpdate indexUpdate : indexUpdates) {
            if (indexUpdate.isCompositeIndex()) {
                final IndexUpdate<StaticBuffer,Entry> update = indexUpdate;
                if (!indexUpdate.isDeletion())
                    mutator.mutateIndex(update.getKey(), Collections.singletonList(update.getEntry()), KCVSCache.NO_DELETIONS);
                else
                    mutator.mutateIndex(update.getKey(), KeyColumnValueStore.NO_ADDITIONS, Collections.singletonList(update.getEntry()));
            } else {
                final IndexUpdate<String,IndexEntry> update = indexUpdate;
                // cdc-only mixed indexes reach this loop only for relation-document DELETIONS (see
                // commitDeletionIndexAppliesToFilter); their additions/refreshes were excluded at generation and are
                // applied asynchronously by the CDC worker instead.
                has2iMods = true;
                IndexTransaction itx = mutator.getIndexTransaction(update.getIndex().getBackingIndexName());
                String indexStore = ((MixedIndexType)update.getIndex()).getStoreName();
                if (!indexUpdate.isDeletion())
                    itx.add(indexStore, update.getKey(), update.getEntry(), update.getElement().isNew());
                else
                    itx.delete(indexStore,update.getKey(),update.getEntry().field,update.getEntry().value,update.getElement().isRemoved());
                //The document of an existing element may turn out to be missing from the index. Hand the provider the
                //element's complete indexed content once, so that it recreates the document whole rather than from the
                //touched fields. Whether the document is an existing one is the pending mutation's to say, not the
                //element's: a property change on an edge or a vertex property replaces the relation, so its deletion
                //arrives from a removed element and its addition from a new one, while the mutation they add up to is
                //an update of the existing document. Read here, while the transaction can still read: the index
                //commit runs after the storage commit. A cdc-only index writes its documents asynchronously and takes
                //none
                if (!isCdcOnlyMixedIndex(update.getIndex()) && itx.needsCompleteDocument(indexStore, update.getKey())) {
                    itx.registerCompleteDocument(indexStore, update.getKey(),
                        indexSerializer.getCompleteDocument(update.getElement(), (MixedIndexType) update.getIndex()));
                }
            }
        }

        return has2iMods;
    }

    private static final Predicate<InternalRelation> SCHEMA_FILTER =
        internalRelation -> internalRelation.getType() instanceof BaseRelationType && internalRelation.getVertex(0) instanceof JanusGraphSchemaVertex;

    private static final Predicate<InternalRelation> NO_SCHEMA_FILTER = internalRelation -> !SCHEMA_FILTER.test(internalRelation);

    private static final Predicate<InternalRelation> NO_FILTER = internalRelation -> true;

    public void commit(final Collection<InternalRelation> addedRelations,
                     final Collection<InternalRelation> deletedRelations, final StandardJanusGraphTx tx) throws BackendException {
        if (addedRelations.isEmpty() && deletedRelations.isEmpty()) return;
        //1. Finalize transaction
        log.debug("Saving transaction. Added {}, removed {}", addedRelations.size(), deletedRelations.size());
        if (!tx.getConfiguration().hasCommitTime()) tx.getConfiguration().setCommitTime(times.getTime());
        final Instant txTimestamp = tx.getConfiguration().getCommitTime();
        final long transactionId = txCounter.incrementAndGet();

        //2. Assign JanusGraphVertex IDs
        if (!tx.getConfiguration().hasAssignIDsImmediately())
            idAssigner.assignIDs(addedRelations);

        //3. Commit
        BackendTransaction mutator = tx.getTxHandle();
        final boolean acquireLocks = tx.getConfiguration().hasAcquireLocks();
        final boolean hasTxIsolation = backend.getStoreFeatures().hasTxIsolation();
        final boolean logTransaction = config.hasLogTransactions() && !tx.getConfiguration().hasEnabledBatchLoading();
        final KCVSLog txLog = logTransaction?backend.getSystemTxLog():null;
        final TransactionLogHeader txLogHeader = new TransactionLogHeader(transactionId,txTimestamp, times);
        ModificationSummary commitSummary;
        final ChangedSchemaVertices changedSchemaVertices = ChangedSchemaVertices.of(addedRelations, deletedRelations);

        try {
            //3.1 Log transaction (write-ahead log) if enabled
            if (logTransaction) {
                //[FAILURE] Inability to log transaction fails the transaction by escalation since it's likely due to unavailability of primary
                //storage backend.
                Preconditions.checkNotNull(txLog, "Transaction log is null");
                txLog.add(txLogHeader.serializeModifications(serializer, LogTxStatus.PRECOMMIT, tx, addedRelations, deletedRelations),txLogHeader.getLogKey());
            }

            //3.2 Commit schema elements and their associated relations in a separate transaction if backend does not support
            //    transactional isolation
            boolean hasSchemaElements = deletedRelations.stream().anyMatch(SCHEMA_FILTER)
                || addedRelations.stream().anyMatch(SCHEMA_FILTER);
            Preconditions.checkArgument(!hasSchemaElements || (!tx.getConfiguration().hasEnabledBatchLoading() && acquireLocks),
                    "Attempting to create schema elements in inconsistent state");

            if (hasSchemaElements && !hasTxIsolation) {
                /*
                 * On storage without transactional isolation, create separate
                 * backend transaction for schema aspects to make sure that
                 * those are persisted prior to and independently of other
                 * mutations in the tx. If the storage supports transactional
                 * isolation, then don't create a separate tx.
                 */
                final BackendTransaction schemaMutator = openBackendTransaction(tx);

                try {
                    //[FAILURE] If the preparation throws an exception abort directly - nothing persisted since batch-loading cannot be enabled for schema elements
                    commitSummary = prepareCommit(addedRelations,deletedRelations, SCHEMA_FILTER, schemaMutator, tx, acquireLocks);
                    assert commitSummary.hasModifications && !commitSummary.has2iModifications;
                } catch (Throwable e) {
                    //Roll back schema tx and escalate exception
                    schemaMutator.rollback();
                    throw e;
                }

                try {
                    schemaMutator.commit();
                } catch (Throwable e) {
                    //[FAILURE] Primary persistence failed => abort and escalate exception, nothing should have been persisted
                    log.error("Could not commit transaction ["+transactionId+"] due to storage exception in system-commit",e);
                    throw e;
                }
            }

            //[FAILURE] Exceptions during preparation here cause the entire transaction to fail on transactional systems
            //or just the non-system part on others. Nothing has been persisted unless batch-loading
            commitSummary = prepareCommit(addedRelations,deletedRelations, hasTxIsolation? NO_FILTER : NO_SCHEMA_FILTER, mutator, tx, acquireLocks);
            if (commitSummary.hasModifications) {
                String logTxIdentifier = tx.getConfiguration().getLogIdentifier();
                boolean hasSecondaryPersistence = logTxIdentifier!=null || commitSummary.has2iModifications;

                //1. Commit storage - failures lead to immediate abort

                //1a. Add success message to tx log which will be committed atomically with all transactional changes so that we can recover secondary failures
                //    This should not throw an exception since the mutations are just cached. If it does, it will be escalated since its critical
                if (logTransaction) {
                    txLog.add(txLogHeader.serializePrimary(serializer,
                                        hasSecondaryPersistence?LogTxStatus.PRIMARY_SUCCESS:LogTxStatus.COMPLETE_SUCCESS),
                            txLogHeader.getLogKey(),mutator.getTxLogPersistor());
                }

                try {
                    mutator.commitStorage();
                } catch (Throwable e) {
                    //[FAILURE] If primary storage persistence fails abort directly (only schema could have been persisted)
                    log.error("Could not commit transaction ["+transactionId+"] due to storage exception in commit",e);
                    throw e;
                }

                if (hasSecondaryPersistence) {
                    LogTxStatus status = LogTxStatus.SECONDARY_SUCCESS;
                    Map<String,Throwable> indexFailures = Collections.emptyMap();
                    boolean userlogSuccess = true;

                    try {
                        //2. Commit indexes - [FAILURE] all exceptions are collected and logged but nothing is aborted
                        indexFailures = mutator.commitIndexes();
                        if (!indexFailures.isEmpty()) {
                            status = LogTxStatus.SECONDARY_FAILURE;
                            for (Map.Entry<String,Throwable> entry : indexFailures.entrySet()) {
                                log.error("Error while committing index mutations for transaction ["+transactionId+"] on index: " +entry.getKey(),entry.getValue());
                            }
                        }
                        //3. Log transaction if configured - [FAILURE] is recorded but does not cause exception
                        if (logTxIdentifier!=null) {
                            try {
                                userlogSuccess = false;
                                final Log userLog = backend.getUserLog(logTxIdentifier);
                                Future<Message> env = userLog.add(txLogHeader.serializeModifications(serializer, LogTxStatus.USER_LOG, tx, addedRelations, deletedRelations));
                                if (env.isDone()) {
                                    try {
                                        env.get();
                                    } catch (ExecutionException ex) {
                                        throw ex.getCause();
                                    }
                                }
                                userlogSuccess=true;
                            } catch (Throwable e) {
                                status = LogTxStatus.SECONDARY_FAILURE;
                                log.error("Could not user-log committed transaction ["+transactionId+"] to " + logTxIdentifier, e);
                            }
                        }
                    } finally {
                        if (logTransaction) {
                            //[FAILURE] An exception here will be logged and not escalated; tx considered success and
                            // needs to be cleaned up later
                            try {
                                txLog.add(txLogHeader.serializeSecondary(serializer,status,indexFailures,userlogSuccess),txLogHeader.getLogKey());
                            } catch (Throwable e) {
                                log.error("Could not tx-log secondary persistence status on transaction ["+transactionId+"]",e);
                            }
                        }
                    }
                } else {
                    //This just closes the transaction since there are no modifications
                    mutator.commitIndexes();
                }
            } else { //Just commit everything at once
                //[FAILURE] This case only happens when there are no non-system mutations in which case all changes
                //are already flushed. Hence, an exception here is unlikely and should abort
                mutator.commit();
            }
        } catch (Throwable e) {
            log.error("Could not commit transaction ["+transactionId+"] due to exception",e);
            try {
                //Clean up any left-over transaction handles
                mutator.rollback();
            } catch (Throwable e2) {
                log.error("Could not roll-back transaction ["+transactionId+"] after failure due to exception",e2);
            }
            throw e;
        } finally {
            //4. Definition edges which no management eviction covers (constraints auto-created under
            //   schema.constraints=true, connections and properties added through addConnection and addProperties,
            //   by a transaction or by ManagementSystem) leave the schema caches stale. Expire the local schema cache
            //   entries of the schema vertices they touch so this instance re-reads them, and those vertices in
            //   the other open transactions, as ManagementSystem does, so that a transaction which was open during
            //   this commit does not keep acting on the definition edges it had loaded. The committing transaction
            //   is left out: it wrote the edges itself and is finishing. So are the vertices it removed, which have
            //   no definition left to reload. The open transactions are only listed once the graph-level entries
            //   are gone, so that a transaction which is not on the list can only load the new definition edges.
            //   A management commit which changes definition edges gets the same from ManagementSystem.commit once
            //   more, at the cost of a re-read...
            if (!changedSchemaVertices.all.isEmpty()) {
                for (Long schemaId : changedSchemaVertices.all) {
                    schemaCache.expireSchemaElement(schemaId);
                }
                for (JanusGraphTransaction other : getOpenTransactions()) {
                    if (other != tx && other.isOpen()) expireSchemaElements(other, changedSchemaVertices.kept);
                }
            }
            //5. ...and tell the other instances to expire those it did not remove, with an eviction which nothing
            //   waits to see acknowledged. Every instance expires them when it reads it, this one included, which so
            //   expires them once more. A removed one (say the old modifier vertex of a consistency which the commit
            //   replaced) has no definition left for anyone to reload, and nothing looks it up once the types it
            //   belonged to are reloaded. A management commit which changes definition edges and has management
            //   evictions of its own sends both.
            if (!changedSchemaVertices.kept.isEmpty()) {
                tellInstancesToExpire(changedSchemaVertices.kept);
            }
        }
    }

    //Runs in the finally block of a commit, so a failure is logged instead of thrown, where it would replace the
    //outcome of the commit. Expiring an element reloads its definition, which can fail like any read; the element's
    //caches are cleared by then, and it is reloaded when the transaction next uses it.
    private static void expireSchemaElements(JanusGraphTransaction transaction, Set<Long> schemaIds) {
        for (Long schemaId : schemaIds) {
            try {
                transaction.expireSchemaElement(schemaId);
            } catch (RuntimeException e) {
                log.warn("Could not expire schema element {} in transaction {}; it is reloaded when the transaction "
                    + "next uses it", schemaId, transaction, e);
            }
        }
    }

    //The instances only need to re-read what they had cached, so nothing waits for them to confirm it, and a failure
    //to tell them does not fail the commit, which has persisted everything it could by now (and runs this after a
    //failure as well, since a backend without transaction isolation may have committed its schema part): they catch
    //up when the elements are evicted next, or when they restart. The management log writes synchronously, so a
    //struggling backend can hold the commit here for up to its max-write-time.
    private void tellInstancesToExpire(final Set<Long> schemaIds) {
        try {
            managementLogger.sendUnacknowledgedCacheEviction(schemaIds);
        } catch (Exception e) {
            log.warn("Could not tell the other instances to expire the schema elements {}, whose definition edges "
                + "this transaction changed. They keep what they had cached until these are evicted again.",
                schemaIds, e);
        }
    }

    /**
     * The schema vertices at the ends of the definition edges a transaction wrote: all of them, whose graph-level
     * cache entries are expired, and those the transaction did not remove, which the other open transactions reload
     * and the other instances are told to expire. A removed one has no definition left to reload.
     */
    private static final class ChangedSchemaVertices {
        private Set<Long> all = Collections.emptySet();
        private Set<Long> kept = Collections.emptySet();

        private static ChangedSchemaVertices of(final Collection<InternalRelation> addedRelations,
                                                final Collection<InternalRelation> deletedRelations) {
            final ChangedSchemaVertices changed = new ChangedSchemaVertices();
            for (Collection<InternalRelation> relations : Arrays.asList(addedRelations, deletedRelations)) {
                for (InternalRelation relation : relations) {
                    if (relation.getType() != BaseLabel.SchemaDefinitionEdge) continue;
                    for (int pos = 0; pos < relation.getArity(); pos++) {
                        InternalVertex vertex = relation.getVertex(pos);
                        if (vertex instanceof JanusGraphSchemaVertex) changed.add((JanusGraphSchemaVertex) vertex);
                    }
                }
            }
            return changed;
        }

        private void add(JanusGraphSchemaVertex vertex) {
            if (all.isEmpty()) {
                all = new HashSet<>();
                kept = new HashSet<>();
            }
            all.add(vertex.longId());
            if (!vertex.isRemoved()) kept.add(vertex.longId());
        }
    }


    private static class ShutdownThread extends Thread {
        private final StandardJanusGraph graph;

        public ShutdownThread(StandardJanusGraph graph) {
            this.graph = graph;
        }

        @Override
        public void start() {
            log.debug("Shutting down graph {} using shutdown hook {}", graph, this);

            graph.closeInternal();
            graph.shutdownHook = null;
        }
    }
}
