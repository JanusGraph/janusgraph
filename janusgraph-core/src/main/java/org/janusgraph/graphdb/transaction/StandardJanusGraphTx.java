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

package org.janusgraph.graphdb.transaction;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Connection;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.MixedIndexCountQuery;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.ReadOnlyTransactionException;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.JanusGraphSchemaElement;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.SchemaInspector;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.indexing.IndexTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.query.graph.MixedIndexCountQueryBuilder;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.IDPool;
import org.janusgraph.graphdb.database.serialize.AttributeHandler;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.InternalVertexLabel;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.query.MetricsQueryExecutor;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.query.QueryExecutor;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.ConditionUtil;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.query.graph.GraphCentricQuery;
import org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder;
import org.janusgraph.graphdb.query.graph.IndexQueryBuilder;
import org.janusgraph.graphdb.query.graph.JointIndexQuery;
import org.janusgraph.graphdb.query.index.IndexSelectionStrategy;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.MultiVertexCentricQueryBuilder;
import org.janusgraph.graphdb.query.vertex.VertexCentricQuery;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.relations.RelationComparator;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.relations.RelationIdentifierUtils;
import org.janusgraph.graphdb.relations.StandardEdge;
import org.janusgraph.graphdb.relations.StandardVertexProperty;
import org.janusgraph.graphdb.tinkerpop.JanusGraphBlueprintsTransaction;
import org.janusgraph.graphdb.transaction.addedrelations.AddedRelationsContainer;
import org.janusgraph.graphdb.transaction.addedrelations.ConcurrentAddedRelations;
import org.janusgraph.graphdb.transaction.addedrelations.SimpleAddedRelations;
import org.janusgraph.graphdb.transaction.indexcache.ConcurrentIndexCache;
import org.janusgraph.graphdb.transaction.indexcache.IndexCache;
import org.janusgraph.graphdb.transaction.indexcache.SimpleIndexCache;
import org.janusgraph.graphdb.transaction.lock.CombinerLock;
import org.janusgraph.graphdb.transaction.lock.FakeLock;
import org.janusgraph.graphdb.transaction.lock.IndexLockTuple;
import org.janusgraph.graphdb.transaction.lock.LockTuple;
import org.janusgraph.graphdb.transaction.lock.ReentrantTransactionLock;
import org.janusgraph.graphdb.transaction.lock.TransactionLock;
import org.janusgraph.graphdb.transaction.subquerycache.GuavaSubqueryCache;
import org.janusgraph.graphdb.transaction.subquerycache.SubqueryCache;
import org.janusgraph.graphdb.transaction.vertexcache.GuavaVertexCache;
import org.janusgraph.graphdb.transaction.vertexcache.VertexCache;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.StandardEdgeLabelMaker;
import org.janusgraph.graphdb.types.StandardPropertyKeyMaker;
import org.janusgraph.graphdb.types.StandardVertexLabelMaker;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionDescription;
import org.janusgraph.graphdb.types.TypeDefinitionMap;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.TypeUtil;
import org.janusgraph.graphdb.types.VertexLabelVertex;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.system.BaseVertexLabel;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.graphdb.types.system.SystemRelationType;
import org.janusgraph.graphdb.types.system.SystemTypeManager;
import org.janusgraph.graphdb.types.vertices.EdgeLabelVertex;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex;
import org.janusgraph.graphdb.util.IndexHelper;
import org.janusgraph.graphdb.util.ProfiledIterator;
import org.janusgraph.graphdb.util.SubqueryIterator;
import org.janusgraph.graphdb.util.VertexCentricEdgeIterable;
import org.janusgraph.graphdb.vertices.CacheVertex;
import org.janusgraph.graphdb.vertices.PreloadedVertex;
import org.janusgraph.graphdb.vertices.StandardVertex;
import org.janusgraph.util.datastructures.Retriever;
import org.janusgraph.util.stats.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardJanusGraphTx extends JanusGraphBlueprintsTransaction implements TypeInspector, SchemaInspector, VertexFactory {

    private static final Logger log = LoggerFactory.getLogger(StandardJanusGraphTx.class);

    private static final Map<Long, InternalRelation> EMPTY_DELETED_RELATIONS = ImmutableMap.of();
    private static final ConcurrentMap<LockTuple, TransactionLock> UNINITIALIZED_LOCKS = null;
    private static final Duration LOCK_TIMEOUT = Duration.ofMillis(5000L);

    /**
     * This is a workaround for #893.  Cache sizes small relative to the level
     * of thread parallelism can lead to JanusGraph generating multiple copies of
     * a single vertex in a single transaction.
     */
    private static final long MIN_VERTEX_CACHE_SIZE = 100L;


    private final StandardJanusGraph graph;
    private final TransactionConfiguration config;
    private final IDManager idManager;
    private final IDManager idInspector;
    private final AttributeHandler attributeHandler;
    private BackendTransaction txHandle;
    private final EdgeSerializer edgeSerializer;
    private final IndexSerializer indexSerializer;
    private final IndexSelectionStrategy indexSelector;

    /* ###############################################
            Internal Data Structures
     ############################################### */

    //####### Vertex Cache
    /**
     * Keeps track of vertices already loaded in memory. Cannot release vertices with added relations.
     */
    private VertexCache vertexCache;

    //######## Data structures that keep track of new and deleted elements
    //These data structures cannot release elements, since we would loose track of what was added or deleted
    /**
     * Keeps track of all added relations in this transaction
     */
    private AddedRelationsContainer addedRelations;
    /**
     * Keeps track of all deleted relations in this transaction
     */
    private volatile Map<Long, InternalRelation> deletedRelations;

    //######## Index Caches
    /**
     * Caches the result of index calls so that repeated index queries don't need
     * to be passed to the IndexProvider. This cache will drop entries when it overflows
     * since the result set can always be retrieved from the IndexProvider
     */
    private SubqueryCache indexCache;
    /**
     * Builds an inverted index for newly added properties so they can be considered in index queries.
     * This cache my not release elements since that would entail an expensive linear scan over addedRelations
     */
    private IndexCache newVertexIndexEntries;

    //######## Lock applications
    /**
     * Transaction-local data structure for unique lock applications so that conflicting applications can be discovered
     * at the transactional level.
     */
    private volatile ConcurrentMap<LockTuple, TransactionLock> uniqueLocks;

    //####### Other Data structures
    /**
     * Caches JanusGraph types by name so that they can be quickly retrieved once they are loaded in the transaction.
     * Since type retrieval by name is common and there are only a few types, since cache is a simple map (i.e. no release)
     */
    private Map<String, Long> newTypeCache;

    /**
     * Used to assign temporary ids to new vertices and relations added in this transaction.
     * If ids are assigned immediately, this is not used. This IDPool is shared across all elements.
     */
    private final IDPool temporaryIds;

    /**
     * This belongs in JanusGraphConfig.
     */
    private final TimestampProvider times;

    /**
     * Whether or not this transaction is open
     */
    private volatile boolean isOpen;

    private final VertexConstructor existingVertexRetriever;
    private final VertexConstructor externalVertexRetriever;
    private final VertexConstructor internalVertexRetriever;

    public StandardJanusGraphTx(StandardJanusGraph graph, TransactionConfiguration config) {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(graph.isOpen());
        Preconditions.checkNotNull(config);
        this.graph = graph;
        this.times = graph.getConfiguration().getTimestampProvider();
        this.config = config;
        this.idManager = graph.getIDManager();
        this.idInspector = idManager;
//        this.idInspector = idManager.getIdInspector();
        this.attributeHandler = graph.getDataSerializer();
        this.edgeSerializer = graph.getEdgeSerializer();
        this.indexSerializer = graph.getIndexSerializer();
        this.indexSelector = graph.getIndexSelector();

        temporaryIds = new IDPool() {

            private final AtomicLong counter = new AtomicLong(1);

            @Override
            public long nextID() {
                return counter.getAndIncrement();
            }

            @Override
            public void close() {
                //Do nothing
            }
        };

        int concurrencyLevel;
        if (config.isSingleThreaded()) {
            addedRelations = new SimpleAddedRelations();
            concurrencyLevel = 1;
            newTypeCache = new HashMap<>();
            newVertexIndexEntries = new SimpleIndexCache();
        } else {
            addedRelations = new ConcurrentAddedRelations();
            concurrencyLevel = 1; //TODO: should we increase this?
            newTypeCache = new NonBlockingHashMap<>();
            newVertexIndexEntries = new ConcurrentIndexCache();
        }

        boolean preloadedData = config.hasPreloadedData();
        externalVertexRetriever = new VertexConstructor(config.hasVerifyExternalVertexExistence(), preloadedData);
        internalVertexRetriever = new VertexConstructor(config.hasVerifyInternalVertexExistence(), preloadedData);
        existingVertexRetriever = new VertexConstructor(false, preloadedData);

        long effectiveVertexCacheSize = config.getVertexCacheSize();
        if (!config.isReadOnly()) {
            effectiveVertexCacheSize = Math.max(MIN_VERTEX_CACHE_SIZE, effectiveVertexCacheSize);
            log.debug("Guava vertex cache size: requested={} effective={} (min={})",
                    config.getVertexCacheSize(), effectiveVertexCacheSize, MIN_VERTEX_CACHE_SIZE);
        }

        vertexCache = new GuavaVertexCache(effectiveVertexCacheSize,concurrencyLevel,config.getDirtyVertexSize());

        indexCache = new GuavaSubqueryCache(concurrencyLevel, config.getIndexCacheWeight());

        uniqueLocks = UNINITIALIZED_LOCKS;
        deletedRelations = EMPTY_DELETED_RELATIONS;

        this.isOpen = true;
        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "begin").inc();
            elementProcessor = new MetricsQueryExecutor<>(config.getGroupName(), "graph", elementProcessorImpl);
            edgeProcessor    = new MetricsQueryExecutor<>(config.getGroupName(), "vertex", edgeProcessorImpl);
        } else {
            elementProcessor = elementProcessorImpl;
            edgeProcessor    = edgeProcessorImpl;
        }
    }

    public void setBackendTransaction(BackendTransaction txHandle) {
        Preconditions.checkArgument(this.txHandle==null && txHandle!=null);
        this.txHandle = txHandle;
    }

    /*
     * ------------------------------------ Utility Access Verification methods ------------------------------------
     */

    private void verifyWriteAccess(JanusGraphVertex... vertices) {
        if (config.isReadOnly())
            throw new ReadOnlyTransactionException("Cannot create new entities in read-only transaction");
        for (JanusGraphVertex v : vertices) {
            if (v.hasId() && idInspector.isUnmodifiableVertex(v.longId()) && !v.isNew())
                throw new SchemaViolationException("Cannot modify unmodifiable vertex: "+v);
        }
        verifyAccess(vertices);
    }

    public final void verifyAccess(JanusGraphVertex... vertices) {
        verifyOpen();
        for (JanusGraphVertex v : vertices) {
            Preconditions.checkArgument(v instanceof InternalVertex, "Invalid vertex: %s", v);
            if (!(v instanceof SystemRelationType) && this != ((InternalVertex) v).tx())
                throw new IllegalStateException("The vertex or type is not associated with this transaction [" + v + "]");
            if (v.isRemoved())
                throw new IllegalStateException("The vertex or type has been removed [" + v + "]");
        }
    }

    private void verifyOpen() {
        if (isClosed())
            throw new IllegalStateException("Operation cannot be executed because the enclosing transaction is closed");
    }

    /*
     * ------------------------------------ External Access ------------------------------------
     */

    public StandardJanusGraphTx getNextTx() {
        Preconditions.checkArgument(isClosed());
        if (!config.isThreadBound())
            throw new IllegalStateException("Cannot access element because its enclosing transaction is closed and unbound");
        else return (StandardJanusGraphTx) graph.getCurrentThreadTx();
    }

    public TransactionConfiguration getConfiguration() {
        return config;
    }

    @Override
    public StandardJanusGraph getGraph() {
        return graph;
    }

    public BackendTransaction getTxHandle() {
        return txHandle;
    }

    public EdgeSerializer getEdgeSerializer() {
        return edgeSerializer;
    }

    public IDManager getIdInspector() {
        return idInspector;
    }

    public boolean isPartitionedVertex(JanusGraphVertex vertex) {
        return vertex.hasId() && idInspector.isPartitionedVertex(vertex.longId());
    }

    public InternalVertex getCanonicalVertex(InternalVertex partitionedVertex) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertex));
        long canonicalId = idManager.getCanonicalVertexId(partitionedVertex.longId());
        if (canonicalId==partitionedVertex.longId()) return partitionedVertex;
        else return getExistingVertex(canonicalId);
    }

    public InternalVertex getOtherPartitionVertex(JanusGraphVertex partitionedVertex, long otherPartition) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertex));
        return getExistingVertex(idManager.getPartitionedVertexId(partitionedVertex.longId(), otherPartition));
    }

    public InternalVertex[] getAllRepresentatives(JanusGraphVertex partitionedVertex, boolean restrict2Partitions) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertex));
        long[] ids;
        if (!restrict2Partitions || !config.hasRestrictedPartitions()) {
            ids = idManager.getPartitionedVertexRepresentatives(partitionedVertex.longId());
        } else {
            int[] restrictedPartitions = config.getRestrictedPartitions();
            ids = new long[restrictedPartitions.length];
            for (int i=0;i<ids.length;i++) {
                ids[i]=idManager.getPartitionedVertexId(partitionedVertex.longId(),restrictedPartitions[i]);
            }
        }
        Preconditions.checkArgument(ids.length>0);
        InternalVertex[] vertices = new InternalVertex[ids.length];
        for (int i=0;i<ids.length;i++) vertices[i]=getExistingVertex(ids[i]);
        return vertices;
    }


    /*
     * ------------------------------------ Vertex Handling ------------------------------------
     */

    public boolean containsVertex(final long vertexId) {
        return getVertex(vertexId) != null;
    }

    private boolean isValidVertexId(long id) {
        return id>0 && (idInspector.isSchemaVertexId(id) || idInspector.isUserVertexId(id));
    }

    @Override
    public JanusGraphVertex getVertex(long vertexId) {
        verifyOpen();
        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getVertexByID").inc();
        }
        if (!isValidVertexId(vertexId)) return null;
        //Make canonical partitioned vertex id
        if (idInspector.isPartitionedVertex(vertexId)) vertexId=idManager.getCanonicalVertexId(vertexId);

        final InternalVertex v = vertexCache.get(vertexId, externalVertexRetriever);
        return (null == v || v.isRemoved()) ? null : v;
    }

    @Override
    public Iterable<JanusGraphVertex> getVertices(long... ids) {
        verifyOpen();
        if (ids==null || ids.length==0) return (Iterable)getInternalVertices();

        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getVerticesByID").inc();
        }
        final List<JanusGraphVertex> result = new ArrayList<>(ids.length);
        final LongArrayList vertexIds = new LongArrayList(ids.length);
        for (long id : ids) {
            if (isValidVertexId(id)) {
                if (idInspector.isPartitionedVertex(id)) id=idManager.getCanonicalVertexId(id);
                if (vertexCache.contains(id))
                    result.add(vertexCache.get(id, existingVertexRetriever));
                else
                    vertexIds.add(id);
            }
        }
        if (!vertexIds.isEmpty()) {
            if (externalVertexRetriever.hasVerifyExistence()) {
                List<EntryList> existence = graph.edgeMultiQuery(vertexIds,graph.vertexExistenceQuery,txHandle);
                for (int i = 0; i < vertexIds.size(); i++) {
                    if (!existence.get(i).isEmpty()) {
                        long id = vertexIds.get(i);
                        result.add(vertexCache.get(id, existingVertexRetriever));
                    }
                }
            } else {
                for (int i = 0; i < vertexIds.size(); i++) {
                    result.add(vertexCache.get(vertexIds.get(i),externalVertexRetriever));
                }
            }
        }
        //Filter out potentially removed vertices
        result.removeIf(JanusGraphElement::isRemoved);
        return result;
    }

    private InternalVertex getExistingVertex(long vertexId) {
        //return vertex no matter what, even if deleted, and assume the id has the correct format
        return vertexCache.get(vertexId, existingVertexRetriever);
    }

    public InternalVertex getInternalVertex(long vertexId) {
        //return vertex but potentially check for existence
        return vertexCache.get(vertexId, internalVertexRetriever);
    }

    private class VertexConstructor implements Retriever<Long, InternalVertex> {

        private final boolean verifyExistence;
        private final boolean createStubVertex;

        private VertexConstructor(boolean verifyExistence, boolean createStubVertex) {
            this.verifyExistence = verifyExistence;
            this.createStubVertex = createStubVertex;
        }

        public boolean hasVerifyExistence() {
            return verifyExistence;
        }

        @Override
        public InternalVertex get(Long vertexId) {
            Preconditions.checkArgument(vertexId!=null && vertexId > 0, "Invalid vertex id: %s",vertexId);
            Preconditions.checkArgument(idInspector.isSchemaVertexId(vertexId) || idInspector.isUserVertexId(vertexId), "Not a valid vertex id: %s", vertexId);

            byte lifecycle = ElementLifeCycle.Loaded;
            long canonicalVertexId = idInspector.isPartitionedVertex(vertexId)?idManager.getCanonicalVertexId(vertexId):vertexId;
            if (verifyExistence) {
                if (graph.edgeQuery(canonicalVertexId, graph.vertexExistenceQuery, txHandle).isEmpty())
                    lifecycle = ElementLifeCycle.Removed;
            }
            if (canonicalVertexId!=vertexId) {
                //Take lifecycle from canonical representative
                lifecycle = getExistingVertex(canonicalVertexId).getLifeCycle();
            }

            final InternalVertex vertex;
            if (idInspector.isRelationTypeId(vertexId)) {
                if (idInspector.isPropertyKeyId(vertexId)) {
                    if (IDManager.isSystemRelationTypeId(vertexId)) {
                        vertex = SystemTypeManager.getSystemType(vertexId);
                    } else {
                        vertex = new PropertyKeyVertex(StandardJanusGraphTx.this, vertexId, lifecycle);
                    }
                } else {
                    assert idInspector.isEdgeLabelId(vertexId);
                    if (IDManager.isSystemRelationTypeId(vertexId)) {
                        vertex = SystemTypeManager.getSystemType(vertexId);
                    } else {
                        vertex = new EdgeLabelVertex(StandardJanusGraphTx.this, vertexId, lifecycle);
                    }
                }
            } else if (idInspector.isVertexLabelVertexId(vertexId)) {
                vertex = new VertexLabelVertex(StandardJanusGraphTx.this,vertexId, lifecycle);
            } else if (idInspector.isGenericSchemaVertexId(vertexId)) {
                vertex = new JanusGraphSchemaVertex(StandardJanusGraphTx.this,vertexId, lifecycle);
            } else if (idInspector.isUserVertexId(vertexId)) {
                if (createStubVertex) vertex = new PreloadedVertex(StandardJanusGraphTx.this, vertexId, lifecycle);
                else vertex = new CacheVertex(StandardJanusGraphTx.this, vertexId, lifecycle);
            } else throw new IllegalArgumentException("ID could not be recognized");
            return vertex;
        }
    }

    @Override
    public JanusGraphVertex addVertex(Long vertexId, VertexLabel label) {
        verifyWriteAccess();
        if (label==null) label=BaseVertexLabel.DEFAULT_VERTEXLABEL;
        if (vertexId != null && !graph.getConfiguration().allowVertexIdSetting()) {
            log.info("Provided vertex id [{}] is ignored because vertex id setting is not enabled", vertexId);
            vertexId = null;
        }
        Preconditions.checkArgument(vertexId != null || !graph.getConfiguration().allowVertexIdSetting(), "Must provide vertex id");
        Preconditions.checkArgument(vertexId == null || IDManager.VertexIDType.NormalVertex.is(vertexId), "Not a valid vertex id: %s", vertexId);
        Preconditions.checkArgument(vertexId == null || ((InternalVertexLabel)label).hasDefaultConfiguration(), "Cannot only use default vertex labels: %s",label);
        Preconditions.checkArgument(vertexId == null || !config.hasVerifyExternalVertexExistence() || !containsVertex(vertexId), "Vertex with given id already exists: %s", vertexId);
        StandardVertex vertex = new StandardVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.NormalVertex, temporaryIds.nextID()), ElementLifeCycle.New);
        if (vertexId != null) {
            vertex.setId(vertexId);
        } else if (config.hasAssignIDsImmediately() || label.isPartitioned()) {
            graph.assignID(vertex,label);
        }
        addProperty(vertex, BaseKey.VertexExists, Boolean.TRUE);
        if (label!=BaseVertexLabel.DEFAULT_VERTEXLABEL) { //Add label
            Preconditions.checkArgument(label instanceof VertexLabelVertex);
            addEdge(vertex, label, BaseLabel.VertexLabelEdge);
        }
        vertexCache.add(vertex, vertex.longId());
        return vertex;

    }

    @Override
    public JanusGraphVertex addVertex(String vertexLabel) {
        return addVertex(getOrCreateVertexLabel(vertexLabel));
    }

    public JanusGraphVertex addVertex(VertexLabel vertexLabel) {
        return addVertex(null, vertexLabel);
    }

    private Iterable<InternalVertex> getInternalVertices() {
        Iterable<InternalVertex> allVertices;
        if (!addedRelations.isEmpty()) {
            //There are possible new vertices
            List<InternalVertex> newVs = vertexCache.getAllNew();
            newVs.removeIf(internalVertex -> internalVertex instanceof JanusGraphSchemaElement);
            allVertices = Iterables.concat(newVs, new VertexIterable(graph, this));
        } else {
            allVertices = new VertexIterable(graph, this);
        }
        //Filter out all but one PartitionVertex representative
        return Iterables.filter(allVertices,
            internalVertex -> !isPartitionedVertex(internalVertex) || internalVertex.longId() == idInspector.getCanonicalVertexId(internalVertex.longId()));
    }


    /*
     * ------------------------------------ Adding and Removing Relations ------------------------------------
     */

    public final boolean validDataType(Class datatype) {
        return attributeHandler.validDataType(datatype);
    }

    public final Object verifyAttribute(PropertyKey key, Object attribute) {
        Class<?> datatype = key.dataType();
        if (datatype.equals(Object.class)) {
            if (!attributeHandler.validDataType(attribute.getClass())) {
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(attribute);
            }
        } else {
            if (!attribute.getClass().equals(datatype)) {

                Object converted = null;
                try {
                    converted = attributeHandler.convert(datatype, attribute);
                } catch (IllegalArgumentException e) {
                    //Just means that data could not be converted
                }
                if (converted == null) throw new SchemaViolationException(
                        "Value [%s] is not an instance of the expected data type for property key [%s] and cannot be converted. Expected: %s, found: %s", attribute,
                        key.name(), datatype, attribute.getClass());
                attribute = converted;
            }
            assert (attribute.getClass().equals(datatype));
            attributeHandler.verifyAttribute(datatype, attribute);
        }
        return attribute;
    }

    public void removeRelation(InternalRelation relation) {
        Preconditions.checkArgument(!relation.isRemoved());
        relation = relation.it();
        for (int i = 0; i < relation.getLen(); i++)
            verifyWriteAccess(relation.getVertex(i));

        //Delete from Vertex
        for (int i = 0; i < relation.getLen(); i++) {
            InternalVertex vertex = relation.getVertex(i);
            vertex.removeRelation(relation);
            if (!vertex.isNew()) {
                vertexCache.add(vertex, vertex.longId());
            }
        }
        //Update transaction data structures
        if (relation.isNew()) {
            addedRelations.remove(relation);
            if (TypeUtil.hasSimpleInternalVertexKeyIndex(relation)) newVertexIndexEntries.remove((JanusGraphVertexProperty) relation);
        } else {
            Preconditions.checkArgument(relation.isLoaded());
            Map<Long, InternalRelation> result = deletedRelations;
            if (result == EMPTY_DELETED_RELATIONS) {
                if (config.isSingleThreaded()) {
                    deletedRelations = result = new HashMap<>();
                } else {
                    synchronized (this) {
                        result = deletedRelations;
                        if (result == EMPTY_DELETED_RELATIONS)
                            deletedRelations = result = new ConcurrentHashMap<>();
                    }
                }
            }
            result.put(relation.longId(), relation);
        }
    }

    public boolean isRemovedRelation(Long relationId) {
        return deletedRelations.containsKey(relationId);
    }

    private TransactionLock getLock(final Object... tuple) {
        return getLock(new LockTuple(tuple));
    }

    private TransactionLock getLock(final LockTuple la) {
        if (config.isSingleThreaded()) return FakeLock.INSTANCE;
        ConcurrentMap<LockTuple, TransactionLock> result = uniqueLocks;
        if (result == UNINITIALIZED_LOCKS) {
            Preconditions.checkArgument(!config.isSingleThreaded());
            synchronized (this) {
                result = uniqueLocks;
                if (result == UNINITIALIZED_LOCKS)
                    uniqueLocks = result = new ConcurrentHashMap<>();
            }
        }
        //TODO: clean out no longer used locks from uniqueLocks when it grows to large (use ReadWriteLock to protect against race conditions)
        TransactionLock lock = new ReentrantTransactionLock();
        TransactionLock existingLock = result.putIfAbsent(la, lock);
        return (existingLock == null)?lock:existingLock;
    }

    private TransactionLock getUniquenessLock(final JanusGraphVertex out, final InternalRelationType type, final Object in) {
        Multiplicity multiplicity = type.multiplicity();
        TransactionLock uniqueLock = FakeLock.INSTANCE;
        if (config.hasVerifyUniqueness() && multiplicity.isConstrained()) {
            uniqueLock = null;
            if (multiplicity==Multiplicity.SIMPLE) {
                uniqueLock = getLock(out, type, in);
            } else {
                for (Direction dir : Direction.proper) {
                    if (multiplicity.isUnique(dir)) {
                        TransactionLock lock = getLock(dir == Direction.OUT ? out : in, type, dir);
                        if (uniqueLock==null) uniqueLock=lock;
                        else uniqueLock=new CombinerLock(uniqueLock,lock,times);
                    }
                }
            }
        }
        assert uniqueLock!=null;
        return uniqueLock;
    }

    private void checkPropertyConstraintForVertexOrCreatePropertyConstraint(JanusGraphVertex vertex, PropertyKey key) {
        if (config.hasDisabledSchemaConstraints()) return;
        VertexLabel vertexLabel = vertex.vertexLabel();
        if (vertexLabel instanceof BaseVertexLabel) return;
        Collection<PropertyKey> propertyKeys = vertexLabel.mappedProperties();
        if (propertyKeys.contains(key)) return;
        config.getAutoSchemaMaker().makePropertyConstraintForVertex(vertexLabel, key, this);
    }

    public void checkPropertyConstraintForEdgeOrCreatePropertyConstraint(StandardEdge edge, PropertyKey key) {
        if (config.hasDisabledSchemaConstraints()) return;
        EdgeLabel edgeLabel = edge.edgeLabel();
        if (edgeLabel instanceof BaseLabel) return;
        Collection<PropertyKey> propertyKeys = edgeLabel.mappedProperties();
        if (propertyKeys.contains(key)) return;
        config.getAutoSchemaMaker().makePropertyConstraintForEdge(edgeLabel, key, this);
    }

    private void checkConnectionConstraintOrCreateConnectionConstraint(JanusGraphVertex outVertex, JanusGraphVertex inVertex, EdgeLabel edgeLabel) {
        if (config.hasDisabledSchemaConstraints()) return;

        VertexLabel outVertexLabel = outVertex.vertexLabel();
        if (outVertexLabel instanceof BaseVertexLabel) return;

        VertexLabel inVertexLabel = inVertex.vertexLabel();
        if (inVertexLabel instanceof BaseVertexLabel) return;

        Collection<Connection> connections = outVertexLabel.mappedConnections();
        for (Connection connection : connections) {
            if (connection.getIncomingVertexLabel() != inVertexLabel) continue;
            if (connection.getEdgeLabel().equals(edgeLabel.name())) return;
        }
        config.getAutoSchemaMaker().makeConnectionConstraint(edgeLabel, outVertexLabel, inVertexLabel, this);
    }

    public JanusGraphEdge addEdge(JanusGraphVertex outVertex, JanusGraphVertex inVertex, EdgeLabel label) {
       return addEdge(null, outVertex, inVertex, label);
    }

    public JanusGraphEdge addEdge(Long id, JanusGraphVertex outVertex, JanusGraphVertex inVertex, EdgeLabel label) {
        verifyWriteAccess(outVertex, inVertex);
        outVertex = ((InternalVertex) outVertex).it();
        inVertex = ((InternalVertex) inVertex).it();
        Preconditions.checkNotNull(label);
        checkConnectionConstraintOrCreateConnectionConstraint(outVertex, inVertex, label);
        Multiplicity multiplicity = label.multiplicity();
        TransactionLock uniqueLock = getUniquenessLock(outVertex, (InternalRelationType) label,inVertex);
        uniqueLock.lock(LOCK_TIMEOUT);
        try {
            //Check uniqueness
            if (config.hasVerifyUniqueness()) {
                if (multiplicity==Multiplicity.SIMPLE) {
                    if (!Iterables.isEmpty(query(outVertex).type(label).direction(Direction.OUT).adjacent(inVertex).edges()))
                            throw new SchemaViolationException("An edge with the given label already exists between the pair of vertices and the label [%s] is simple", label.name());
                }
                if (multiplicity.isUnique(Direction.OUT)) {
                    if (!Iterables.isEmpty(query(outVertex).type(label).direction(Direction.OUT).edges()))
                            throw new SchemaViolationException("An edge with the given label already exists on the out-vertex and the label [%s] is out-unique", label.name());
                }
                if (multiplicity.isUnique(Direction.IN)) {
                    if (!Iterables.isEmpty(query(inVertex).type(label).direction(Direction.IN).edges()))
                            throw new SchemaViolationException("An edge with the given label already exists on the in-vertex and the label [%s] is in-unique", label.name());
                }
            }
            long edgeId = id == null ? IDManager.getTemporaryRelationID(temporaryIds.nextID()) : id;
            StandardEdge edge = new StandardEdge(edgeId, label, (InternalVertex) outVertex, (InternalVertex) inVertex, ElementLifeCycle.New);
            if (config.hasAssignIDsImmediately() && id == null) graph.assignID(edge);
            connectRelation(edge);
            return edge;
        } finally {
            uniqueLock.unlock();
        }
    }

    private void connectRelation(InternalRelation r) {
        for (int i = 0; i < r.getLen(); i++) {
            boolean success = r.getVertex(i).addRelation(r);
            if (!success) throw new AssertionError("Could not connect relation: " + r);
        }
        addedRelations.add(r);
        for (int pos = 0; pos < r.getLen(); pos++) {
            InternalVertex vertex = r.getVertex(pos);
            if (!vertex.isNew()) {
                vertexCache.add(vertex, vertex.longId());
            }
        }
        if (TypeUtil.hasSimpleInternalVertexKeyIndex(r)) {
            newVertexIndexEntries.add((JanusGraphVertexProperty) r);
        }
    }

    public JanusGraphVertexProperty addProperty(JanusGraphVertex vertex, PropertyKey key, Object value) {
        return addProperty(vertex, key, value, null);
    }

    public JanusGraphVertexProperty addProperty(JanusGraphVertex vertex, PropertyKey key, Object value, Long id) {
        return addProperty(key.cardinality().convert(), vertex, key, value, id);
    }

    public JanusGraphVertexProperty addProperty(VertexProperty.Cardinality cardinality, JanusGraphVertex vertex, PropertyKey key, Object value) {
        return addProperty(cardinality, vertex, key, value, null);
    }

    public JanusGraphVertexProperty addProperty(VertexProperty.Cardinality cardinality, JanusGraphVertex vertex, PropertyKey key, Object value, Long id) {
        if (key.cardinality().convert()!=cardinality && cardinality!=VertexProperty.Cardinality.single)
            throw new SchemaViolationException("Key is defined for %s cardinality which conflicts with specified: %s",key.cardinality(),cardinality);
        verifyWriteAccess(vertex);
        Preconditions.checkArgument(!(key instanceof ImplicitKey),"Cannot create a property of implicit type: %s",key.name());
        vertex = ((InternalVertex) vertex).it();
        Preconditions.checkNotNull(key);
        checkPropertyConstraintForVertexOrCreatePropertyConstraint(vertex, key);
        final Object normalizedValue = verifyAttribute(key, value);
        Cardinality keyCardinality = key.cardinality();

        //Determine unique indexes
        final List<IndexLockTuple> uniqueIndexTuples = new ArrayList<>();
        for (CompositeIndexType index : TypeUtil.getUniqueIndexes(key)) {
            IndexSerializer.IndexRecords matches = IndexSerializer.indexMatches(vertex, index, key, normalizedValue);
            for (Object[] match : matches.getRecordValues()) uniqueIndexTuples.add(new IndexLockTuple(index,match));
        }

        TransactionLock uniqueLock = getUniquenessLock(vertex, (InternalRelationType) key, normalizedValue);
        //Add locks for unique indexes
        for (IndexLockTuple lockTuple : uniqueIndexTuples) uniqueLock = new CombinerLock(uniqueLock,getLock(lockTuple),times);
        uniqueLock.lock(LOCK_TIMEOUT);
        try {
//            //Check vertex-centric uniqueness -> this doesn't really make sense to check
//            if (config.hasVerifyUniqueness()) {
//                if (cardinality == Cardinality.SINGLE) {
//                    if (!Iterables.isEmpty(query(vertex).type(key).properties()))
//                        throw new SchemaViolationException("A property with the given key [%s] already exists on the vertex [%s] and the property key is defined as single-valued", key.name(), vertex);
//                }
//                if (cardinality == Cardinality.SET) {
//                    if (!Iterables.isEmpty(Iterables.filter(query(vertex).type(key).properties(), new Predicate<JanusGraphVertexProperty>() {
//                        @Override
//                        public boolean apply(@Nullable JanusGraphVertexProperty janusgraphProperty) {
//                            return normalizedValue.equals(janusgraphProperty.value());
//                        }
//                    })))
//                        throw new SchemaViolationException("A property with the given key [%s] and value [%s] already exists on the vertex and the property key is defined as set-valued", key.name(), normalizedValue);
//                }
//            }

            long propId = id == null ? IDManager.getTemporaryRelationID(temporaryIds.nextID()) : id;
            StandardVertexProperty prop = new StandardVertexProperty(propId, key, (InternalVertex) vertex, normalizedValue, ElementLifeCycle.New);
            if (config.hasAssignIDsImmediately() && id == null) graph.assignID(prop);

            //Delete properties if the cardinality is restricted
            if (cardinality==VertexProperty.Cardinality.single || cardinality== VertexProperty.Cardinality.set) {
                Consumer<JanusGraphVertexProperty> propertyRemover = JanusGraphVertexProperty.getRemover(cardinality, normalizedValue);
                /* If we are simply overwriting a vertex property, then we don't have to explicitly remove it thereby saving a read operation
                   However, this only applies if
                   1) we don't lock on the property key or consistency checks are disabled and
                   2) there are no indexes for this property key
                   3) the cardinalities match (if we overwrite a set with single, we need to read all other values to delete)
                */

                if ( (!config.hasVerifyUniqueness() || ((InternalRelationType)key).getConsistencyModifier()!=ConsistencyModifier.LOCK) &&
                        !TypeUtil.hasAnyIndex(key) && cardinality==keyCardinality.convert()) {
                    //Only delete in-memory so as to not trigger a read from the database which isn't necessary because we will overwrite blindly
                    //We need to label the new property as "upsert", so that in case property deletion happens, we not only delete this new
                    //in-memory property, but also read from database to delete the old value (if exists)
                    ((InternalVertex) vertex).getAddedRelations(p -> p.getType().equals(key)).forEach(p -> propertyRemover.accept((JanusGraphVertexProperty) p));
                    prop.setUpsert(true);
                } else {
                    ((InternalVertex) vertex).query().types(key).properties().forEach(propertyRemover);
                }
            }

            //Check index uniqueness
            if (config.hasVerifyUniqueness()) {
                //Check all unique indexes
                for (IndexLockTuple lockTuple : uniqueIndexTuples) {
                    if (!Iterables.isEmpty(IndexHelper.getQueryResults(lockTuple.getIndex(), lockTuple.getAll(), this)))
                        throw new SchemaViolationException("Adding this property for key [%s] and value [%s] violates a uniqueness constraint [%s]", key.name(), normalizedValue, lockTuple.getIndex());
                }
            }
            connectRelation(prop);
            return prop;
        } finally {
            uniqueLock.unlock();
        }

    }


    @Override
    public Iterable<JanusGraphEdge> getEdges(RelationIdentifier... ids) {
        verifyOpen();
        if (ids==null || ids.length==0) return new VertexCentricEdgeIterable(getInternalVertices(),RelationCategory.EDGE);

        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getEdgesByID").inc();
        }
        List<JanusGraphEdge> result = new ArrayList<>(ids.length);
        for (RelationIdentifier id : ids) {
            if (id==null) continue;
            JanusGraphEdge edge = RelationIdentifierUtils.findEdge(id,this);
            if (edge!=null && !edge.isRemoved()) result.add(edge);
        }
        return result;
    }


    /*
     * ------------------------------------ Schema Handling ------------------------------------
     */

    public final JanusGraphSchemaVertex makeSchemaVertex(JanusGraphSchemaCategory schemaCategory, String name, TypeDefinitionMap definition) {
        verifyOpen();
        Preconditions.checkArgument(!schemaCategory.hasName() || StringUtils.isNotBlank(name), "Need to provide a valid name for type [%s]", schemaCategory);
        schemaCategory.verifyValidDefinition(definition);
        JanusGraphSchemaVertex schemaVertex;
        if (schemaCategory.isRelationType()) {
            if (schemaCategory == JanusGraphSchemaCategory.PROPERTYKEY) {
                schemaVertex = new PropertyKeyVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.UserPropertyKey, temporaryIds.nextID()), ElementLifeCycle.New);
            } else {
                assert schemaCategory == JanusGraphSchemaCategory.EDGELABEL;
                schemaVertex = new EdgeLabelVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.UserEdgeLabel,temporaryIds.nextID()), ElementLifeCycle.New);
            }
        } else if (schemaCategory==JanusGraphSchemaCategory.VERTEXLABEL) {
            schemaVertex = new VertexLabelVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.GenericSchemaType,temporaryIds.nextID()), ElementLifeCycle.New);
        } else {
            schemaVertex = new JanusGraphSchemaVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.GenericSchemaType,temporaryIds.nextID()), ElementLifeCycle.New);
        }

        graph.assignID(schemaVertex, BaseVertexLabel.DEFAULT_VERTEXLABEL);
        Preconditions.checkArgument(schemaVertex.longId() > 0);
        if (schemaCategory.hasName()) addProperty(schemaVertex, BaseKey.SchemaName, schemaCategory.getSchemaName(name));
        addProperty(schemaVertex, BaseKey.VertexExists, Boolean.TRUE);
        addProperty(schemaVertex, BaseKey.SchemaCategory, schemaCategory);
        updateSchemaVertex(schemaVertex);
        for (Map.Entry<TypeDefinitionCategory,Object> def : definition.entrySet()) {
            JanusGraphVertexProperty p = addProperty(schemaVertex, BaseKey.SchemaDefinitionProperty, def.getValue());
            p.property(BaseKey.SchemaDefinitionDesc.name(), TypeDefinitionDescription.of(def.getKey()));
        }
        vertexCache.add(schemaVertex, schemaVertex.longId());
        if (schemaCategory.hasName()) newTypeCache.put(schemaCategory.getSchemaName(name), schemaVertex.longId());
        return schemaVertex;

    }

    public void updateSchemaVertex(JanusGraphSchemaVertex schemaVertex) {
        addProperty(VertexProperty.Cardinality.single, schemaVertex, BaseKey.SchemaUpdateTime, times.getTime(times.getTime()));
    }

    public PropertyKey makePropertyKey(String name, TypeDefinitionMap definition) {
        return (PropertyKey) makeSchemaVertex(JanusGraphSchemaCategory.PROPERTYKEY, name, definition);
    }

    public EdgeLabel makeEdgeLabel(String name, TypeDefinitionMap definition) {
        return (EdgeLabel) makeSchemaVertex(JanusGraphSchemaCategory.EDGELABEL, name, definition);
    }


    public JanusGraphEdge addSchemaEdge(JanusGraphVertex out, JanusGraphVertex in, TypeDefinitionCategory def, Object modifier) {
        assert def.isEdge();
        JanusGraphEdge edge = addEdge(out, in, BaseLabel.SchemaDefinitionEdge);
        TypeDefinitionDescription desc = new TypeDefinitionDescription(def, modifier);
        edge.property(BaseKey.SchemaDefinitionDesc.name(), desc);
        return edge;
    }

    @Override
    public VertexLabel addProperties(VertexLabel vertexLabel, PropertyKey... keys) {
        for (PropertyKey key : keys) {
            addSchemaEdge(vertexLabel, key, TypeDefinitionCategory.PROPERTY_KEY_EDGE, null);
        }
        return vertexLabel;
    }

    @Override
    public EdgeLabel addProperties(EdgeLabel edgeLabel, PropertyKey... keys) {
        for (PropertyKey key : keys) {
            if (key.cardinality() != Cardinality.SINGLE) {
                throw new IllegalArgumentException(String.format("An Edge [%s] can not have a property [%s] with the cardinality [%s].", edgeLabel, key, key.cardinality()));
            }
            addSchemaEdge(edgeLabel, key, TypeDefinitionCategory.PROPERTY_KEY_EDGE, null);
        }
        return edgeLabel;
    }

    @Override
    public EdgeLabel addConnection(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel) {
        addSchemaEdge(outVLabel, inVLabel, TypeDefinitionCategory.CONNECTION_EDGE, edgeLabel.name());
        addSchemaEdge(edgeLabel, outVLabel, TypeDefinitionCategory.UPDATE_CONNECTION_EDGE, null);
        return edgeLabel;
    }

    public JanusGraphSchemaVertex getSchemaVertex(String schemaName) {
        Long schemaId = newTypeCache.get(schemaName);
        if (schemaId==null) schemaId=graph.getSchemaCache().getSchemaId(schemaName);
        if (schemaId != null) {
            InternalVertex typeVertex = vertexCache.get(schemaId, existingVertexRetriever);
            assert typeVertex!=null;
            return (JanusGraphSchemaVertex)typeVertex;
        } else return null;
    }

    @Override
    public boolean containsRelationType(String name) {
        return getRelationType(name)!=null;
    }

    @Override
    public RelationType getRelationType(String name) {
        verifyOpen();

        RelationType type = SystemTypeManager.getSystemType(name);
        if (type!=null) return type;

        return (RelationType)getSchemaVertex(JanusGraphSchemaCategory.getRelationTypeName(name));
    }

    @Override
    public boolean containsPropertyKey(String name) {
        RelationType type = getRelationType(name);
        return type!=null && type.isPropertyKey();
    }

    @Override
    public boolean containsEdgeLabel(String name) {
        RelationType type = getRelationType(name);
        return type!=null && type.isEdgeLabel();
    }

    // this is critical path we can't allow anything heavier then assertion in here
    @Override
    public RelationType getExistingRelationType(long typeId) {
        assert idInspector.isRelationTypeId(typeId);
        if (IDManager.isSystemRelationTypeId(typeId)) {
            return SystemTypeManager.getSystemType(typeId);
        } else {
            InternalVertex v = getInternalVertex(typeId);
            return (RelationType) v;
        }
    }

    @Override
    public PropertyKey getPropertyKey(String name) {
        RelationType pk = getRelationType(name);
        Preconditions.checkArgument(pk==null || pk.isPropertyKey(), "The relation type with name [%s] is not a property key",name);
        return (PropertyKey)pk;
    }

    @Override
    public PropertyKey getOrCreatePropertyKey(String name, Object value) {
        return getOrCreatePropertyKey(name, value, null);
    }

    @Override
    public PropertyKey getOrCreatePropertyKey(String name, Object value, VertexProperty.Cardinality cardinality) {
        RelationType et = getRelationType(name);
        if (et == null) {
            PropertyKeyMaker propertyKeyMaker = makePropertyKey(name);
            if (cardinality != null) {
                propertyKeyMaker = propertyKeyMaker.cardinality(cardinality);
            }
            return config.getAutoSchemaMaker().makePropertyKey(propertyKeyMaker, value);
        } else if (et.isPropertyKey()) {
            return (PropertyKey) et;
        } else {
            throw new IllegalArgumentException("The type of given name is not a key: " + name);
        }
    }

    @Override
    public PropertyKey getOrCreatePropertyKey(String name) {
        RelationType et = getRelationType(name);
        if (et == null) {
            return config.getAutoSchemaMaker().makePropertyKey(makePropertyKey(name));
        } else if (et.isPropertyKey()) {
            return (PropertyKey) et;
        } else {
            throw new IllegalArgumentException("The type of given name is not a key: " + name);
        }
    }

    @Override
    public EdgeLabel getEdgeLabel(String name) {
        RelationType el = getRelationType(name);
        Preconditions.checkArgument(el==null || el.isEdgeLabel(), "The relation type with name [%s] is not an edge label",name);
        return (EdgeLabel)el;
    }

    @Override
    public EdgeLabel getOrCreateEdgeLabel(String name) {
        RelationType et = getRelationType(name);
        if (et == null) {
            return config.getAutoSchemaMaker().makeEdgeLabel(makeEdgeLabel(name));
        } else if (et.isEdgeLabel()) {
            return (EdgeLabel) et;
        } else
            throw new IllegalArgumentException("The type of given name is not a label: " + name);
    }

    @Override
    public PropertyKeyMaker makePropertyKey(String name) {
        return new StandardPropertyKeyMaker(this, name, indexSerializer, attributeHandler);
    }

    @Override
    public EdgeLabelMaker makeEdgeLabel(String name) {
        return new StandardEdgeLabelMaker(this, name, indexSerializer, attributeHandler);
    }

    //-------- Vertex Labels -----------------

    @Override
    public VertexLabel getExistingVertexLabel(long id) {
        assert idInspector.isVertexLabelVertexId(id);
        InternalVertex v = getInternalVertex(id);
        return (VertexLabelVertex)v;
    }

    @Override
    public boolean containsVertexLabel(String name) {
        verifyOpen();
        return BaseVertexLabel.DEFAULT_VERTEXLABEL.name().equals(name) || getSchemaVertex(JanusGraphSchemaCategory.VERTEXLABEL.getSchemaName(name)) != null;

    }

    @Override
    public VertexLabel getVertexLabel(String name) {
        verifyOpen();
        if (BaseVertexLabel.DEFAULT_VERTEXLABEL.name().equals(name)) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        return (VertexLabel)getSchemaVertex(JanusGraphSchemaCategory.VERTEXLABEL.getSchemaName(name));
    }

    @Override
    public VertexLabel getOrCreateVertexLabel(String name) {
        VertexLabel vertexLabel = getVertexLabel(name);
        if (vertexLabel==null) {
            vertexLabel = config.getAutoSchemaMaker().makeVertexLabel(makeVertexLabel(name));
        }
        return vertexLabel;
    }

    @Override
    public VertexLabelMaker makeVertexLabel(String name) {
        StandardVertexLabelMaker maker = new StandardVertexLabelMaker(this);
        maker.name(name);
        return maker;
    }
    /*
     * ------------------------------------ Query Answering ------------------------------------
     */

    public VertexCentricQueryBuilder query(JanusGraphVertex vertex) {
        return new VertexCentricQueryBuilder(((InternalVertex) vertex).it());
    }

    @Override
    public JanusGraphMultiVertexQuery multiQuery(JanusGraphVertex... vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this);
        for (JanusGraphVertex v : vertices) builder.addVertex(v);
        return builder;
    }

    @Override
    public JanusGraphMultiVertexQuery multiQuery(Collection<JanusGraphVertex> vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this);
        builder.addAllVertices(vertices);
        return builder;
    }

    public void executeMultiQuery(final Collection<InternalVertex> vertices, final SliceQuery sq, final QueryProfiler profiler) {
        LongArrayList vertexIds = new LongArrayList(vertices.size());
        for (InternalVertex v : vertices) {
            if (!v.isNew() && v.hasId() && (v instanceof CacheVertex) && !v.hasLoadedRelations(sq)) vertexIds.add(v.longId());
        }

        if (!vertexIds.isEmpty()) {
            List<EntryList> results = QueryProfiler.profile(profiler, sq, true, q -> graph.edgeMultiQuery(vertexIds, q, txHandle));
            int pos = 0;
            for (JanusGraphVertex v : vertices) {
                if (pos<vertexIds.size() && vertexIds.get(pos) == v.longId()) {
                    final EntryList vresults = results.get(pos);
                    ((CacheVertex) v).loadRelations(sq, query -> vresults);
                    pos++;
                }
            }
        }
    }

    public final QueryExecutor<VertexCentricQuery, JanusGraphRelation, SliceQuery> edgeProcessor;

    public final QueryExecutor<VertexCentricQuery, JanusGraphRelation, SliceQuery> edgeProcessorImpl = new QueryExecutor<VertexCentricQuery, JanusGraphRelation, SliceQuery>() {

        @Override
        public Iterator<JanusGraphRelation> getNew(final VertexCentricQuery query) {
            InternalVertex vertex = query.getVertex();
            if (vertex.isNew() || vertex.hasAddedRelations()) {
                return getMatchedRelations(query, vertex);
            } else {
                return Collections.emptyIterator();
            }
        }

        private Iterator<JanusGraphRelation> getMatchedRelations(final VertexCentricQuery query, final InternalVertex vertex) {
            // Need to filter out self-loops if query only asks for one direction
            return new Iterator<JanusGraphRelation>() {
                Iterator<InternalRelation> iterator = vertex.getAddedRelations(t -> true).iterator();
                InternalRelation loop = null;
                InternalRelation current = null;

                @Override
                public boolean hasNext() {
                    if (current == null) {
                        current = nextRelation();
                        return current != null;
                    }
                    return true;
                }

                @Override
                public JanusGraphRelation next() {
                    if (hasNext()) {
                        InternalRelation current = this.current;
                        this.current = null;
                        return current;
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                private InternalRelation nextRelation() {
                    if (loop != null) {
                        InternalRelation loop = this.loop;
                        this.loop = null;
                        return loop;
                    }
                    while (iterator.hasNext()) {
                        InternalRelation next = iterator.next();
                        if (query.matches(next)) {
                            if (query.getDirection() == Direction.BOTH && next instanceof JanusGraphEdge && next.isLoop()) {
                                loop = next;
                            }
                            return next;
                        }
                    }
                    return null;
                }
            };
        }

        @Override
        public boolean hasDeletions(VertexCentricQuery query) {
            InternalVertex vertex = query.getVertex();
            if (vertex.isNew()) return false;
            //In addition to deleted, we need to also check for added relations since those can potentially
            //replace existing ones due to a multiplicity constraint
            return vertex.hasRemovedRelations() || vertex.hasAddedRelations();
        }

        @Override
        public boolean isDeleted(VertexCentricQuery query, JanusGraphRelation result) {
            if (deletedRelations.containsKey(result.longId()) || result != ((InternalRelation) result).it()) return true;
            //Check if this relation is replaced by an added one due to a multiplicity constraint
            InternalRelationType type = (InternalRelationType)result.getType();
            InternalVertex vertex = query.getVertex();
            if (type.multiplicity().isConstrained() && vertex.hasAddedRelations()) {
                final RelationComparator comparator = new RelationComparator(vertex);
                return !Iterables.isEmpty(vertex.getAddedRelations(internalRelation -> comparator.compare((InternalRelation) result, internalRelation) == 0));
            }
            return false;
        }

        @Override
        public Iterator<JanusGraphRelation> execute(final VertexCentricQuery query, final SliceQuery sq, final Object exeInfo, final QueryProfiler profiler) {
            assert exeInfo==null;
            if (query.getVertex().isNew())
                return Collections.emptyIterator();

            final InternalVertex v = query.getVertex();

            final EntryList iterable = v.loadRelations(sq, query1 -> QueryProfiler.profile(profiler, query1, q -> graph.edgeQuery(v.longId(), q, txHandle)));

            return RelationConstructor.readRelation(v, iterable, StandardJanusGraphTx.this).iterator();
        }
    };

    public final QueryExecutor<GraphCentricQuery, JanusGraphElement, JointIndexQuery> elementProcessor;

    public final QueryExecutor<GraphCentricQuery, JanusGraphElement, JointIndexQuery> elementProcessorImpl = new QueryExecutor<GraphCentricQuery, JanusGraphElement, JointIndexQuery>() {

        private PredicateCondition<PropertyKey, JanusGraphElement> getEqualityCondition(Condition<JanusGraphElement> condition) {
            if (condition instanceof PredicateCondition) {
                final PredicateCondition<PropertyKey, JanusGraphElement> pc = (PredicateCondition) condition;
                if (pc.getPredicate() == Cmp.EQUAL && TypeUtil.hasSimpleInternalVertexKeyIndex(pc.getKey())) return pc;
            } else if (condition instanceof And) {
                for (final Condition<JanusGraphElement> child : condition.getChildren()) {
                    final PredicateCondition<PropertyKey, JanusGraphElement> p = getEqualityCondition(child);
                    if (p != null) return p;
                }
            }
            return null;
        }


        @Override
        public Iterator<JanusGraphElement> getNew(final GraphCentricQuery query) {
            //If the query is unconstrained then we don't need to add new elements, so will be picked up by getVertices()/getEdges() below
            if (query.numSubQueries()==1 && query.getSubQuery(0).getBackendQuery().isEmpty())
                return Collections.emptyIterator();
            Preconditions.checkArgument(query.getCondition().hasChildren(),"If the query is non-empty it needs to have a condition");

            if (query.getResultType() == ElementCategory.VERTEX && hasModifications()) {
                Preconditions.checkArgument(QueryUtil.isQueryNormalForm(query.getCondition()));
                PredicateCondition<PropertyKey, JanusGraphElement> standardIndexKey = getEqualityCondition(query.getCondition());
                Iterator<JanusGraphVertex> vertices;
                if (standardIndexKey == null) {
                    final Set<PropertyKey> keys = Sets.newHashSet();
                    ConditionUtil.traversal(query.getCondition(), cond -> {
                        Preconditions.checkArgument(cond.getType() != Condition.Type.LITERAL || cond instanceof PredicateCondition);
                        if (cond instanceof PredicateCondition) {
                            keys.add(((PredicateCondition<PropertyKey, JanusGraphElement>) cond).getKey());
                        }
                        return true;
                    });
                    Preconditions.checkArgument(!keys.isEmpty(), "Invalid query condition: %s", query.getCondition());
                    Set<JanusGraphVertex> vertexSet = Sets.newHashSet();
                    for (final JanusGraphRelation r : addedRelations.getView(relation -> keys.contains(relation.getType()))) {
                        vertexSet.add(((JanusGraphVertexProperty) r).element());
                    }
                    for (JanusGraphRelation r : deletedRelations.values()) {
                        if (keys.contains(r.getType())) {
                            JanusGraphVertex v = ((JanusGraphVertexProperty) r).element();
                            if (!v.isRemoved()) vertexSet.add(v);
                        }
                    }
                    vertices = vertexSet.iterator();
                } else {
                    vertices = com.google.common.collect.Iterators.transform(newVertexIndexEntries.get(standardIndexKey.getValue(), standardIndexKey.getKey()).iterator(), new com.google.common.base.Function<JanusGraphVertexProperty, JanusGraphVertex>() {
                        @Nullable
                        @Override
                        public JanusGraphVertex apply(final JanusGraphVertexProperty o) {
                            return o.element();
                        }
                    });
                }

                return (Iterator) com.google.common.collect.Iterators.filter(vertices, query::matches);
            } else if ( (query.getResultType() == ElementCategory.EDGE || query.getResultType()==ElementCategory.PROPERTY)
                                        && !addedRelations.isEmpty()) {
                return (Iterator) addedRelations.getView(relation -> query.getResultType().isInstance(relation) && !relation.isInvisible() && query.matches(relation)).iterator();
            } else return Collections.emptyIterator();
        }


        @Override
        public boolean hasDeletions(GraphCentricQuery query) {
            return hasModifications();
        }

        @Override
        public boolean isDeleted(GraphCentricQuery query, JanusGraphElement result) {
            if (result == null || result.isRemoved()) return true;
            else if (query.getResultType() == ElementCategory.VERTEX) {
                Preconditions.checkArgument(result instanceof InternalVertex);
                InternalVertex v = ((InternalVertex) result).it();
                return (v.hasAddedRelations() || v.hasRemovedRelations()) && !query.matches(result);
            } else if (query.getResultType() == ElementCategory.EDGE || query.getResultType()==ElementCategory.PROPERTY) {
                Preconditions.checkArgument(result.isLoaded() || result.isNew());
                //Loaded relations are immutable so we don't need to check those
                //New relations could be modified in this transaction to now longer match the query, hence we need to
                //check for this case and consider the relations deleted
                return result.isNew() && !query.matches(result);
            } else throw new IllegalArgumentException("Unexpected type: " + query.getResultType());
        }

        @Override
        public Iterator<JanusGraphElement> execute(final GraphCentricQuery query, final JointIndexQuery indexQuery, final Object exeInfo, final QueryProfiler profiler) {
            Iterator<JanusGraphElement> iterator;
            if (!indexQuery.isEmpty()) {
                final List<QueryUtil.IndexCall<Object>> retrievals = new ArrayList<>();
                // Leave first index for streaming, and prepare the rest for intersecting and lookup
                for (int i = 1; i < indexQuery.size(); i++) {
                    final JointIndexQuery.Subquery subquery = indexQuery.getQuery(i);
                    retrievals.add(limit -> {
                        final JointIndexQuery.Subquery adjustedQuery = subquery.updateLimit(limit);
                        try {
                            return indexCache.get(adjustedQuery,
                                () -> QueryProfiler.profile(subquery.getProfiler(), adjustedQuery, q -> indexSerializer.query(q, txHandle).collect(Collectors.toList())));
                        } catch (Exception e) {
                            throw new JanusGraphException("Could not call index", e);
                        }
                    });
                }
                // Constructs an iterator which lazily streams results from 1st index, and filters by looking up in the intersection of results from all other indices (if any)
                // NOTE NO_LIMIT is passed to processIntersectingRetrievals to prevent incomplete intersections, which could lead to missed results
                iterator = new SubqueryIterator(indexQuery.getQuery(0), indexSerializer, txHandle, indexCache, indexQuery.getLimit(), getConversionFunction(query.getResultType()),
                        retrievals.isEmpty() ? null: QueryUtil.processIntersectingRetrievals(retrievals, Query.NO_LIMIT));
            } else {
                if (config.hasForceIndexUsage()) throw new JanusGraphException("Could not find a suitable index to answer graph query and graph scans are disabled: " + query);
                log.warn("Query requires iterating over all vertices [{}]. For better performance, use indexes", query.getCondition());

                QueryProfiler sub = profiler.addNested("scan");
                sub.setAnnotation(QueryProfiler.QUERY_ANNOTATION,indexQuery);
                sub.setAnnotation(QueryProfiler.FULLSCAN_ANNOTATION,true);
                sub.setAnnotation(QueryProfiler.CONDITION_ANNOTATION,query.getResultType());

                Supplier<Iterator<JanusGraphElement>> iteratorSupplier;
                switch (query.getResultType()) {
                    case VERTEX:
                        iteratorSupplier = () -> (Iterator) getVertices().iterator();
                        break;
                    case EDGE:
                        iteratorSupplier = () -> (Iterator) getEdges().iterator();
                        break;
                    case PROPERTY:
                        iteratorSupplier = () -> new VertexCentricEdgeIterable(getInternalVertices(),RelationCategory.PROPERTY).iterator();
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected type: " + query.getResultType());
                }
                return new ProfiledIterator<>(sub, iteratorSupplier);
            }

            return iterator;
        }

    };

    public Function<Object, ? extends JanusGraphElement> getConversionFunction(final ElementCategory elementCategory) {
        switch (elementCategory) {
            case VERTEX:
                return vertexIDConversionFct;
            case EDGE:
                return edgeIDConversionFct;
            case PROPERTY:
                return propertyIDConversionFct;
            default:
                throw new IllegalArgumentException("Unexpected result type: " + elementCategory);
        }
    }

    private final Function<Object, JanusGraphVertex> vertexIDConversionFct = id -> {
        Preconditions.checkNotNull(id);
        Preconditions.checkArgument(id instanceof Long);
        return getInternalVertex((Long) id);
    };

    private final Function<Object, JanusGraphEdge> edgeIDConversionFct = id -> {
        Preconditions.checkNotNull(id);
        Preconditions.checkArgument(id instanceof RelationIdentifier);
        return RelationIdentifierUtils.findEdge((RelationIdentifier)id, StandardJanusGraphTx.this);
    };

    private final Function<Object, JanusGraphVertexProperty> propertyIDConversionFct = id -> {
        Preconditions.checkNotNull(id);
        Preconditions.checkArgument(id instanceof RelationIdentifier);
        return RelationIdentifierUtils.findProperty((RelationIdentifier)id, StandardJanusGraphTx.this);
    };

    @Override
    public GraphCentricQueryBuilder query() {
        return new GraphCentricQueryBuilder(this, graph.getIndexSerializer(), indexSelector);
    }

    @Override
    public MixedIndexCountQuery mixedIndexCountQuery() {
       return new MixedIndexCountQueryBuilder(indexSerializer, txHandle);
    }

    @Override
    public JanusGraphIndexQuery indexQuery(String indexName, String query) {
        return new IndexQueryBuilder(this,indexSerializer).setIndex(indexName).setQuery(query);
    }

    /*
     * ------------------------------------ Transaction State ------------------------------------
     */

    @Override
    public synchronized void commit() {
        Preconditions.checkArgument(isOpen(), "The transaction has already been closed");
        boolean success = false;
        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "commit").inc();
        }
        try {
            if (hasModifications()) {
                graph.commit(addedRelations.getAll(), deletedRelations.values(), this);
            } else {
                txHandle.commit();
            }
            success = true;
        } catch (Exception e) {
            try {
                txHandle.rollback();
            } catch (BackendException e1) {
                throw new JanusGraphException("Could not rollback after a failed commit", e);
            }
            throw new JanusGraphException("Could not commit transaction due to exception during persistence", e);
        } finally {
            releaseTransaction();
            if (null != config.getGroupName() && !success) {
                MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "commit.exceptions").inc();
            }
        }
    }

    @Override
    public synchronized void rollback() {
        Preconditions.checkArgument(isOpen(), "The transaction has already been closed");
        boolean success = false;
        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "rollback").inc();
        }
        try {
            txHandle.rollback();
            success = true;
        } catch (Exception e) {
            throw new JanusGraphException("Could not rollback transaction due to exception", e);
        } finally {
            releaseTransaction();
            if (null != config.getGroupName() && !success) {
                MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "rollback.exceptions").inc();
            }
        }
    }

    private void releaseTransaction() {
        isOpen = false;
        graph.closeTransaction(this);
        vertexCache = null;
        indexCache = null;
        addedRelations = null;
        deletedRelations = null;
        uniqueLocks = null;
        newVertexIndexEntries = null;
        newTypeCache = null;
    }

    @Override
    public final boolean isOpen() {
        return isOpen;
    }

    @Override
    public final boolean isClosed() {
        return !isOpen;
    }

    @Override
    public boolean hasModifications() {
        return !addedRelations.isEmpty() || !deletedRelations.isEmpty();
    }

    @Override
    public void expireSchemaElement(final long id) {
        if (vertexCache.contains(id)) {
            final InternalVertex v = vertexCache.get(id, externalVertexRetriever);
            if (v instanceof JanusGraphSchemaVertex) {
                JanusGraphSchemaVertex sv = (JanusGraphSchemaVertex) v;
                sv.resetCache();
                if (sv.getDefinition().containsKey(TypeDefinitionCategory.INTERNAL_INDEX)) {
                    IndexType indexType = sv.asIndexType();
                    if (indexType.isMixedIndex()) {
                        // Invalidate mixed index
                        String store = indexType.getBackingIndexName();
                        if (txHandle.hasIndexTransaction(store)) {
                            IndexTransaction indexTx = txHandle.getIndexTransaction(store);
                            indexTx.invalidate(sv.name());
                        }
                    }
                }
            }
        }
    }
}
