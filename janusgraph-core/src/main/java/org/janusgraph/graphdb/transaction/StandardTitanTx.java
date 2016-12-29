package com.thinkaurelius.titan.graphdb.transaction;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.schema.*;
import com.thinkaurelius.titan.core.schema.SchemaInspector;
import com.thinkaurelius.titan.diskstorage.BackendException;

import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.query.profile.QueryProfiler;
import com.thinkaurelius.titan.graphdb.relations.RelationComparator;
import com.thinkaurelius.titan.graphdb.tinkerpop.TitanBlueprintsTransaction;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPool;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandler;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.*;
import com.thinkaurelius.titan.graphdb.query.*;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.query.graph.GraphCentricQuery;
import com.thinkaurelius.titan.graphdb.query.graph.GraphCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.query.graph.IndexQueryBuilder;
import com.thinkaurelius.titan.graphdb.query.graph.JointIndexQuery;
import com.thinkaurelius.titan.graphdb.query.vertex.MultiVertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQuery;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.relations.StandardEdge;
import com.thinkaurelius.titan.graphdb.relations.StandardVertexProperty;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.AddedRelationsContainer;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.ConcurrentBufferAddedRelations;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.SimpleBufferAddedRelations;
import com.thinkaurelius.titan.graphdb.transaction.indexcache.ConcurrentIndexCache;
import com.thinkaurelius.titan.graphdb.transaction.indexcache.IndexCache;
import com.thinkaurelius.titan.graphdb.transaction.indexcache.SimpleIndexCache;
import com.thinkaurelius.titan.graphdb.transaction.lock.*;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.GuavaVertexCache;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.VertexCache;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.types.system.*;
import com.thinkaurelius.titan.graphdb.types.vertices.EdgeLabelVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.PropertyKeyVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import com.thinkaurelius.titan.graphdb.util.IndexHelper;
import com.thinkaurelius.titan.graphdb.util.VertexCentricEdgeIterable;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.thinkaurelius.titan.graphdb.vertices.PreloadedVertex;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.thinkaurelius.titan.util.stats.MetricManager;
import org.apache.tinkerpop.gremlin.structure.*;

import org.apache.commons.lang.StringUtils;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardTitanTx extends TitanBlueprintsTransaction implements TypeInspector, SchemaInspector, VertexFactory {

    private static final Logger log = LoggerFactory.getLogger(StandardTitanTx.class);

    private static final Map<Long, InternalRelation> EMPTY_DELETED_RELATIONS = ImmutableMap.of();
    private static final ConcurrentMap<LockTuple, TransactionLock> UNINITIALIZED_LOCKS = null;
    private static final Duration LOCK_TIMEOUT = Duration.ofMillis(5000L);

    /**
     * This is a workaround for #893.  Cache sizes small relative to the level
     * of thread parallelism can lead to Titan generating multiple copies of
     * a single vertex in a single transaction.
     */
    private static final long MIN_VERTEX_CACHE_SIZE = 100L;


    private final StandardTitanGraph graph;
    private final TransactionConfiguration config;
    private final IDManager idManager;
    private final IDManager idInspector;
    private final AttributeHandler attributeHandler;
    private BackendTransaction txHandle;
    private final EdgeSerializer edgeSerializer;
    private final IndexSerializer indexSerializer;

    /* ###############################################
            Internal Data Structures
     ############################################### */

    //####### Vertex Cache
    /**
     * Keeps track of vertices already loaded in memory. Cannot release vertices with added relations.
     */
    private final VertexCache vertexCache;

    //######## Data structures that keep track of new and deleted elements
    //These data structures cannot release elements, since we would loose track of what was added or deleted
    /**
     * Keeps track of all added relations in this transaction
     */
    private final AddedRelationsContainer addedRelations;
    /**
     * Keeps track of all deleted relations in this transaction
     */
    private Map<Long, InternalRelation> deletedRelations;

    //######## Index Caches
    /**
     * Caches the result of index calls so that repeated index queries don't need
     * to be passed to the IndexProvider. This cache will drop entries when it overflows
     * since the result set can always be retrieved from the IndexProvider
     */
    private final Cache<JointIndexQuery.Subquery, List<Object>> indexCache;
    /**
     * Builds an inverted index for newly added properties so they can be considered in index queries.
     * This cache my not release elements since that would entail an expensive linear scan over addedRelations
     */
    private final IndexCache newVertexIndexEntries;

    //######## Lock applications
    /**
     * Transaction-local data structure for unique lock applications so that conflicting applications can be discovered
     * at the transactional level.
     */
    private ConcurrentMap<LockTuple, TransactionLock> uniqueLocks;

    //####### Other Data structures
    /**
     * Caches Titan types by name so that they can be quickly retrieved once they are loaded in the transaction.
     * Since type retrieval by name is common and there are only a few types, since cache is a simple map (i.e. no release)
     */
    private final Map<String, Long> newTypeCache;

    /**
     * Used to assign temporary ids to new vertices and relations added in this transaction.
     * If ids are assigned immediately, this is not used. This IDPool is shared across all elements.
     */
    private final IDPool temporaryIds;

    /**
     * This belongs in TitanConfig.
     */
    private final TimestampProvider times;

    /**
     * Whether or not this transaction is open
     */
    private boolean isOpen;

    private final VertexConstructor existingVertexRetriever;
    private final VertexConstructor externalVertexRetriever;
    private final VertexConstructor internalVertexRetriever;

    public StandardTitanTx(StandardTitanGraph graph, TransactionConfiguration config) {
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
            addedRelations = new SimpleBufferAddedRelations();
            concurrencyLevel = 1;
            newTypeCache = new HashMap<String, Long>();
            newVertexIndexEntries = new SimpleIndexCache();
        } else {
            addedRelations = new ConcurrentBufferAddedRelations();
            concurrencyLevel = 1; //TODO: should we increase this?
            newTypeCache = new NonBlockingHashMap<String, Long>();
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

        indexCache = CacheBuilder.newBuilder().weigher(new Weigher<JointIndexQuery.Subquery, List<Object>>() {
            @Override
            public int weigh(JointIndexQuery.Subquery q, List<Object> r) {
                return 2 + r.size();
            }
        }).concurrencyLevel(concurrencyLevel).maximumWeight(config.getIndexCacheWeight()).build();

        uniqueLocks = UNINITIALIZED_LOCKS;
        deletedRelations = EMPTY_DELETED_RELATIONS;

        this.isOpen = true;
        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "begin").inc();
            elementProcessor = new MetricsQueryExecutor<GraphCentricQuery, TitanElement, JointIndexQuery>(config.getGroupName(), "graph", elementProcessorImpl);
            edgeProcessor    = new MetricsQueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery>(config.getGroupName(), "vertex", edgeProcessorImpl);
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

    private void verifyWriteAccess(TitanVertex... vertices) {
        if (config.isReadOnly())
            throw new UnsupportedOperationException("Cannot create new entities in read-only transaction");
        for (TitanVertex v : vertices) {
            if (v.hasId() && idInspector.isUnmodifiableVertex(v.longId()) && !v.isNew())
                throw new SchemaViolationException("Cannot modify unmodifiable vertex: "+v);
        }
        verifyAccess(vertices);
    }

    public final void verifyAccess(TitanVertex... vertices) {
        verifyOpen();
        for (TitanVertex v : vertices) {
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

    public StandardTitanTx getNextTx() {
        Preconditions.checkArgument(isClosed());
        if (!config.isThreadBound())
            throw new IllegalStateException("Cannot access element because its enclosing transaction is closed and unbound");
        else return (StandardTitanTx) graph.getCurrentThreadTx();
    }

    public TransactionConfiguration getConfiguration() {
        return config;
    }

    @Override
    public StandardTitanGraph getGraph() {
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

    public boolean isPartitionedVertex(TitanVertex vertex) {
        return vertex.hasId() && idInspector.isPartitionedVertex(vertex.longId());
    }

    public InternalVertex getCanonicalVertex(InternalVertex partitionedVertex) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertex));
        long canonicalId = idManager.getCanonicalVertexId(partitionedVertex.longId());
        if (canonicalId==partitionedVertex.longId()) return partitionedVertex;
        else return getExistingVertex(canonicalId);
    }

    public InternalVertex getOtherPartitionVertex(TitanVertex partitionedVertex, long otherPartition) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertex));
        return getExistingVertex(idManager.getPartitionedVertexId(partitionedVertex.longId(), otherPartition));
    }

    public InternalVertex[] getAllRepresentatives(TitanVertex partitionedVertex, boolean restrict2Partitions) {
        Preconditions.checkArgument(isPartitionedVertex(partitionedVertex));
        long[] ids;
        if (!restrict2Partitions || !config.hasRestrictedPartitions()) {
            ids = idManager.getPartitionedVertexRepresentatives(partitionedVertex.longId());
        } else {
            int[] restrictedParititions = config.getRestrictedPartitions();
            ids = new long[restrictedParititions.length];
            for (int i=0;i<ids.length;i++) {
                ids[i]=idManager.getPartitionedVertexId(partitionedVertex.longId(),restrictedParititions[i]);
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

    public boolean containsVertex(final long vertexid) {
        return getVertex(vertexid) != null;
    }

    private boolean isValidVertexId(long id) {
        return id>0 && (idInspector.isSchemaVertexId(id) || idInspector.isUserVertexId(id));
    }

    @Override
    public TitanVertex getVertex(long vertexid) {
        verifyOpen();
        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getVertexByID").inc();
        }
        if (!isValidVertexId(vertexid)) return null;
        //Make canonical partitioned vertex id
        if (idInspector.isPartitionedVertex(vertexid)) vertexid=idManager.getCanonicalVertexId(vertexid);

        InternalVertex v = null;
        v = vertexCache.get(vertexid, externalVertexRetriever);
        return (null == v || v.isRemoved()) ? null : v;
    }

    @Override
    public Iterable<TitanVertex> getVertices(long... ids) {
        verifyOpen();
        if (ids==null || ids.length==0) return (Iterable)getInternalVertices();

        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getVerticesByID").inc();
        }
        List<TitanVertex> result = new ArrayList<TitanVertex>(ids.length);
        LongArrayList vids = new LongArrayList(ids.length);
        for (long id : ids) {
            if (isValidVertexId(id)) {
                if (idInspector.isPartitionedVertex(id)) id=idManager.getCanonicalVertexId(id);
                if (vertexCache.contains(id))
                    result.add(vertexCache.get(id, existingVertexRetriever));
                else
                    vids.add(id);
            }
        }
        if (!vids.isEmpty()) {
            if (externalVertexRetriever.hasVerifyExistence()) {
                List<EntryList> existence = graph.edgeMultiQuery(vids,graph.vertexExistenceQuery,txHandle);
                for (int i = 0; i < vids.size(); i++) {
                    if (!existence.get(i).isEmpty()) {
                        long id = vids.get(i);
                        result.add(vertexCache.get(id, existingVertexRetriever));
                    }
                }
            } else {
                for (int i = 0; i < vids.size(); i++) {
                    result.add(vertexCache.get(vids.get(i),externalVertexRetriever));
                }
            }
        }
        //Filter out potentially removed vertices
        for (Iterator<TitanVertex> iterator = result.iterator(); iterator.hasNext(); ) {
            if (iterator.next().isRemoved()) iterator.remove();
        }
        return result;
    }

    private InternalVertex getExistingVertex(long vertexid) {
        //return vertex no matter what, even if deleted, and assume the id has the correct format
        return vertexCache.get(vertexid, existingVertexRetriever);
    }

    public InternalVertex getInternalVertex(long vertexid) {
        //return vertex but potentially check for existence
        return vertexCache.get(vertexid, internalVertexRetriever);
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
        public InternalVertex get(Long vertexid) {
            Preconditions.checkArgument(vertexid!=null && vertexid > 0, "Invalid vertex id: %s",vertexid);
            Preconditions.checkArgument(idInspector.isSchemaVertexId(vertexid) || idInspector.isUserVertexId(vertexid), "Not a valid vertex id: %s", vertexid);

            byte lifecycle = ElementLifeCycle.Loaded;
            long canonicalVertexId = idInspector.isPartitionedVertex(vertexid)?idManager.getCanonicalVertexId(vertexid):vertexid;
            if (verifyExistence) {
                if (graph.edgeQuery(canonicalVertexId, graph.vertexExistenceQuery, txHandle).isEmpty())
                    lifecycle = ElementLifeCycle.Removed;
            }
            if (canonicalVertexId!=vertexid) {
                //Take lifecycle from canonical representative
                lifecycle = getExistingVertex(canonicalVertexId).getLifeCycle();
            }

            InternalVertex vertex = null;
            if (idInspector.isRelationTypeId(vertexid)) {
                if (idInspector.isPropertyKeyId(vertexid)) {
                    if (idInspector.isSystemRelationTypeId(vertexid)) {
                        vertex = SystemTypeManager.getSystemType(vertexid);
                    } else {
                        vertex = new PropertyKeyVertex(StandardTitanTx.this, vertexid, lifecycle);
                    }
                } else {
                    assert idInspector.isEdgeLabelId(vertexid);
                    if (idInspector.isSystemRelationTypeId(vertexid)) {
                        vertex = SystemTypeManager.getSystemType(vertexid);
                    } else {
                        vertex = new EdgeLabelVertex(StandardTitanTx.this, vertexid, lifecycle);
                    }
                }
            } else if (idInspector.isVertexLabelVertexId(vertexid)) {
                vertex = new VertexLabelVertex(StandardTitanTx.this,vertexid, lifecycle);
            } else if (idInspector.isGenericSchemaVertexId(vertexid)) {
                vertex = new TitanSchemaVertex(StandardTitanTx.this,vertexid, lifecycle);
            } else if (idInspector.isUserVertexId(vertexid)) {
                if (createStubVertex) vertex = new PreloadedVertex(StandardTitanTx.this, vertexid, lifecycle);
                else vertex = new CacheVertex(StandardTitanTx.this, vertexid, lifecycle);
            } else throw new IllegalArgumentException("ID could not be recognized");
            return vertex;
        }
    }

    @Override
    public TitanVertex addVertex(Long vertexId, VertexLabel label) {
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
            addEdge(vertex, (VertexLabelVertex) label, BaseLabel.VertexLabelEdge);
        }
        vertexCache.add(vertex, vertex.longId());
        return vertex;

    }

    @Override
    public TitanVertex addVertex(String vertexLabel) {
        return addVertex(getOrCreateVertexLabel(vertexLabel));
    }

    public TitanVertex addVertex(VertexLabel vertexLabel) {
        return addVertex(null, vertexLabel);
    }

    private Iterable<InternalVertex> getInternalVertices() {
        Iterable<InternalVertex> allVertices;
        if (!addedRelations.isEmpty()) {
            //There are possible new vertices
            List<InternalVertex> newVs = vertexCache.getAllNew();
            Iterator<InternalVertex> viter = newVs.iterator();
            while (viter.hasNext()) {
                if (viter.next() instanceof TitanSchemaElement) viter.remove();
            }
            allVertices = Iterables.concat(newVs, new VertexIterable(graph, this));
        } else {
            allVertices = new VertexIterable(graph, this);
        }
        //Filter out all but one PartitionVertex representative
        return Iterables.filter(allVertices, new Predicate<InternalVertex>() {
            @Override
            public boolean apply(@Nullable InternalVertex internalVertex) {
                return !isPartitionedVertex(internalVertex) || internalVertex.longId() == idInspector.getCanonicalVertexId(internalVertex.longId());
            }
        });
    }


    /*
     * ------------------------------------ Adding and Removing Relations ------------------------------------
     */

    public final boolean validDataType(Class datatype) {
        return attributeHandler.validDataType(datatype);
    }

    public final Object verifyAttribute(PropertyKey key, Object attribute) {
        if (attribute==null) throw new SchemaViolationException("Property value cannot be null");
        Class<?> datatype = key.dataType();
        if (datatype.equals(Object.class)) {
            if (!attributeHandler.validDataType(attribute.getClass())) {
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(attribute);
            }
            return attribute;
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
            return attribute;
        }
    }

    public void removeRelation(InternalRelation relation) {
        Preconditions.checkArgument(!relation.isRemoved());
        relation = relation.it();
        for (int i = 0; i < relation.getLen(); i++)
            verifyWriteAccess(relation.getVertex(i));

        //Delete from Vertex
        for (int i = 0; i < relation.getLen(); i++) {
            relation.getVertex(i).removeRelation(relation);
        }
        //Update transaction data structures
        if (relation.isNew()) {
            addedRelations.remove(relation);
            if (TypeUtil.hasSimpleInternalVertexKeyIndex(relation)) newVertexIndexEntries.remove((TitanVertexProperty) relation);
        } else {
            Preconditions.checkArgument(relation.isLoaded());
            if (deletedRelations == EMPTY_DELETED_RELATIONS) {
                if (config.isSingleThreaded()) {
                    deletedRelations = new HashMap<Long, InternalRelation>();
                } else {
                    synchronized (this) {
                        if (deletedRelations == EMPTY_DELETED_RELATIONS)
                            deletedRelations = new ConcurrentHashMap<Long, InternalRelation>();
                    }
                }
            }
            deletedRelations.put(relation.longId(), relation);
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
        if (uniqueLocks == UNINITIALIZED_LOCKS) {
            Preconditions.checkArgument(!config.isSingleThreaded());
            synchronized (this) {
                if (uniqueLocks == UNINITIALIZED_LOCKS)
                    uniqueLocks = new ConcurrentHashMap<LockTuple, TransactionLock>();
            }
        }
        //TODO: clean out no longer used locks from uniqueLocks when it grows to large (use ReadWriteLock to protect against race conditions)
        TransactionLock lock = new ReentrantTransactionLock();
        TransactionLock existingLock = uniqueLocks.putIfAbsent(la, lock);
        return (existingLock == null)?lock:existingLock;
    }

    private TransactionLock getUniquenessLock(final TitanVertex out, final InternalRelationType type, final Object in) {
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


    public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, EdgeLabel label) {
        verifyWriteAccess(outVertex, inVertex);
        outVertex = ((InternalVertex) outVertex).it();
        inVertex = ((InternalVertex) inVertex).it();
        Preconditions.checkNotNull(label);
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
            StandardEdge edge = new StandardEdge(IDManager.getTemporaryRelationID(temporaryIds.nextID()), label, (InternalVertex) outVertex, (InternalVertex) inVertex, ElementLifeCycle.New);
            if (config.hasAssignIDsImmediately()) graph.assignID(edge);
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
        for (int pos = 0; pos < r.getLen(); pos++) vertexCache.add(r.getVertex(pos), r.getVertex(pos).longId());
        if (TypeUtil.hasSimpleInternalVertexKeyIndex(r)) newVertexIndexEntries.add((TitanVertexProperty) r);
    }

    public TitanVertexProperty addProperty(TitanVertex vertex, PropertyKey key, Object value) {
        return addProperty(key.cardinality().convert(), vertex, key, value);
    }

    public TitanVertexProperty addProperty(VertexProperty.Cardinality cardi, TitanVertex vertex, PropertyKey key, Object value) {
        if (key.cardinality().convert()!=cardi && cardi!=VertexProperty.Cardinality.single)
                throw new SchemaViolationException(String.format("Key is defined for %s cardinality which conflicts with specified: %s",key.cardinality(),cardi));
        verifyWriteAccess(vertex);
        Preconditions.checkArgument(!(key instanceof ImplicitKey),"Cannot create a property of implicit type: %s",key.name());
        vertex = ((InternalVertex) vertex).it();
        Preconditions.checkNotNull(key);
        final Object normalizedValue = verifyAttribute(key, value);
        Cardinality cardinality = key.cardinality();

        //Determine unique indexes
        List<IndexLockTuple> uniqueIndexTuples = new ArrayList<IndexLockTuple>();
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
//                    if (!Iterables.isEmpty(Iterables.filter(query(vertex).type(key).properties(), new Predicate<TitanVertexProperty>() {
//                        @Override
//                        public boolean apply(@Nullable TitanVertexProperty titanProperty) {
//                            return normalizedValue.equals(titanProperty.value());
//                        }
//                    })))
//                        throw new SchemaViolationException("A property with the given key [%s] and value [%s] already exists on the vertex and the property key is defined as set-valued", key.name(), normalizedValue);
//                }
//            }

            //Delete properties if the cardinality is restricted
            if (cardi==VertexProperty.Cardinality.single || cardi== VertexProperty.Cardinality.set) {
                Consumer<TitanRelation> propertyRemover;
                if (cardi==VertexProperty.Cardinality.single)
                    propertyRemover = p -> p.remove();
                else
                    propertyRemover = p -> { if (((TitanVertexProperty)p).value().equals(normalizedValue)) p.remove(); };

                /* If we are simply overwriting a vertex property, then we don't have to explicitly remove it thereby saving a read operation
                   However, this only applies if
                   1) we don't lock on the property key or consistency checks are disabled and
                   2) there are no indexes for this property key
                   3) the cardinalities match (if we overwrite a set with single, we need to read all other values to delete)
                */
                ConsistencyModifier mod = ((InternalRelationType)key).getConsistencyModifier();
                boolean hasIndex = TypeUtil.hasAnyIndex(key);
                boolean cardiEqual = cardi==cardinality.convert();

                if ( (!config.hasVerifyUniqueness() || ((InternalRelationType)key).getConsistencyModifier()!=ConsistencyModifier.LOCK) &&
                        !TypeUtil.hasAnyIndex(key) && cardi==cardinality.convert()) {
                    //Only delete in-memory so as to not trigger a read from the database which isn't necessary because we will overwrite blindly
                    ((InternalVertex) vertex).getAddedRelations(p -> p.getType().equals(key)).forEach(propertyRemover);
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
            StandardVertexProperty prop = new StandardVertexProperty(IDManager.getTemporaryRelationID(temporaryIds.nextID()), key, (InternalVertex) vertex, normalizedValue, ElementLifeCycle.New);
            if (config.hasAssignIDsImmediately()) graph.assignID(prop);
            connectRelation(prop);
            return prop;
        } finally {
            uniqueLock.unlock();
        }

    }


    @Override
    public Iterable<TitanEdge> getEdges(RelationIdentifier... ids) {
        verifyOpen();
        if (ids==null || ids.length==0) return new VertexCentricEdgeIterable(getInternalVertices(),RelationCategory.EDGE);

        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getEdgesByID").inc();
        }
        List<TitanEdge> result = new ArrayList<>(ids.length);
        for (RelationIdentifier id : ids) {
            if (id==null) continue;
            TitanEdge edge = id.findEdge(this);
            if (edge!=null && !edge.isRemoved()) result.add(edge);
        }
        return result;
    }


    /*
     * ------------------------------------ Schema Handling ------------------------------------
     */

    public final TitanSchemaVertex makeSchemaVertex(TitanSchemaCategory schemaCategory, String name, TypeDefinitionMap definition) {
        verifyOpen();
        Preconditions.checkArgument(!schemaCategory.hasName() || StringUtils.isNotBlank(name), "Need to provide a valid name for type [%s]", schemaCategory);
        schemaCategory.verifyValidDefinition(definition);
        TitanSchemaVertex schemaVertex;
        if (schemaCategory.isRelationType()) {
            if (schemaCategory == TitanSchemaCategory.PROPERTYKEY) {
                schemaVertex = new PropertyKeyVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.UserPropertyKey, temporaryIds.nextID()), ElementLifeCycle.New);
            } else {
                assert schemaCategory == TitanSchemaCategory.EDGELABEL;
                schemaVertex = new EdgeLabelVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.UserEdgeLabel,temporaryIds.nextID()), ElementLifeCycle.New);
            }
        } else if (schemaCategory==TitanSchemaCategory.VERTEXLABEL) {
            schemaVertex = new VertexLabelVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.GenericSchemaType,temporaryIds.nextID()), ElementLifeCycle.New);
        } else {
            schemaVertex = new TitanSchemaVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.GenericSchemaType,temporaryIds.nextID()), ElementLifeCycle.New);
        }

        graph.assignID(schemaVertex, BaseVertexLabel.DEFAULT_VERTEXLABEL);
        Preconditions.checkArgument(schemaVertex.longId() > 0);
        if (schemaCategory.hasName()) addProperty(schemaVertex, BaseKey.SchemaName, schemaCategory.getSchemaName(name));
        addProperty(schemaVertex, BaseKey.VertexExists, Boolean.TRUE);
        addProperty(schemaVertex, BaseKey.SchemaCategory, schemaCategory);
        updateSchemaVertex(schemaVertex);
        addProperty(schemaVertex, BaseKey.SchemaUpdateTime, times.getTime(times.getTime()));
        for (Map.Entry<TypeDefinitionCategory,Object> def : definition.entrySet()) {
            TitanVertexProperty p = addProperty(schemaVertex, BaseKey.SchemaDefinitionProperty, def.getValue());
            p.property(BaseKey.SchemaDefinitionDesc.name(), TypeDefinitionDescription.of(def.getKey()));
        }
        vertexCache.add(schemaVertex, schemaVertex.longId());
        if (schemaCategory.hasName()) newTypeCache.put(schemaCategory.getSchemaName(name), schemaVertex.longId());
        return schemaVertex;

    }

    public void updateSchemaVertex(TitanSchemaVertex schemaVertex) {
        addProperty(VertexProperty.Cardinality.single, schemaVertex, BaseKey.SchemaUpdateTime, times.getTime(times.getTime()));
    }

    public PropertyKey makePropertyKey(String name, TypeDefinitionMap definition) {
        return (PropertyKey) makeSchemaVertex(TitanSchemaCategory.PROPERTYKEY, name, definition);
    }

    public EdgeLabel makeEdgeLabel(String name, TypeDefinitionMap definition) {
        return (EdgeLabel) makeSchemaVertex(TitanSchemaCategory.EDGELABEL, name, definition);
    }

    public TitanSchemaVertex getSchemaVertex(String schemaName) {
        Long schemaId = newTypeCache.get(schemaName);
        if (schemaId==null) schemaId=graph.getSchemaCache().getSchemaId(schemaName);
        if (schemaId != null) {
            InternalVertex typeVertex = vertexCache.get(schemaId, existingVertexRetriever);
            assert typeVertex!=null;
            return (TitanSchemaVertex)typeVertex;
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

        return (RelationType)getSchemaVertex(TitanSchemaCategory.getRelationTypeName(name));
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
    public RelationType getExistingRelationType(long typeid) {
        assert idInspector.isRelationTypeId(typeid);
        if (idInspector.isSystemRelationTypeId(typeid)) {
            return SystemTypeManager.getSystemType(typeid);
        } else {
            InternalVertex v = getInternalVertex(typeid);
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
    public PropertyKey getOrCreatePropertyKey(String name) {
        RelationType et = getRelationType(name);
        if (et == null) {
            return config.getAutoSchemaMaker().makePropertyKey(makePropertyKey(name));
        } else if (et.isPropertyKey()) {
            return (PropertyKey) et;
        } else
            throw new IllegalArgumentException("The type of given name is not a key: " + name);
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
        StandardPropertyKeyMaker maker = new StandardPropertyKeyMaker(this, name, indexSerializer, attributeHandler);
        return maker;
    }

    @Override
    public EdgeLabelMaker makeEdgeLabel(String name) {
        StandardEdgeLabelMaker maker = new StandardEdgeLabelMaker(this, name, indexSerializer, attributeHandler);
        return maker;
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
        if (BaseVertexLabel.DEFAULT_VERTEXLABEL.name().equals(name)) return true;

        return getSchemaVertex(TitanSchemaCategory.VERTEXLABEL.getSchemaName(name))!=null;
    }

    @Override
    public VertexLabel getVertexLabel(String name) {
        verifyOpen();
        if (BaseVertexLabel.DEFAULT_VERTEXLABEL.name().equals(name)) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        return (VertexLabel)getSchemaVertex(TitanSchemaCategory.VERTEXLABEL.getSchemaName(name));
    }

    @Override
    public VertexLabel getOrCreateVertexLabel(String name) {
        VertexLabel vlabel = getVertexLabel(name);
        if (vlabel==null) {
            vlabel = config.getAutoSchemaMaker().makeVertexLabel(makeVertexLabel(name));
        }
        return vlabel;
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

    public VertexCentricQueryBuilder query(TitanVertex vertex) {
        return new VertexCentricQueryBuilder(((InternalVertex) vertex).it());
    }

    @Override
    @Deprecated
    public TitanMultiVertexQuery multiQuery(TitanVertex... vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this);
        for (TitanVertex v : vertices) builder.addVertex(v);
        return builder;
    }

    @Override
    @Deprecated
    public TitanMultiVertexQuery multiQuery(Collection<TitanVertex> vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this);
        builder.addAllVertices(vertices);
        return builder;
    }

    public void executeMultiQuery(final Collection<InternalVertex> vertices, final SliceQuery sq, final QueryProfiler profiler) {
        LongArrayList vids = new LongArrayList(vertices.size());
        for (InternalVertex v : vertices) {
            if (!v.isNew() && v.hasId() && (v instanceof CacheVertex) && !v.hasLoadedRelations(sq)) vids.add(v.longId());
        }

        if (!vids.isEmpty()) {
            List<EntryList> results = QueryProfiler.profile(profiler, sq, true, q -> graph.edgeMultiQuery(vids, q, txHandle));
            int pos = 0;
            for (TitanVertex v : vertices) {
                if (pos<vids.size() && vids.get(pos) == v.longId()) {
                    final EntryList vresults = results.get(pos);
                    ((CacheVertex) v).loadRelations(sq, new Retriever<SliceQuery, EntryList>() {
                        @Override
                        public EntryList get(SliceQuery query) {
                            return vresults;
                        }
                    });
                    pos++;
                }
            }
        }
    }

    public final QueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery> edgeProcessor;

    public final QueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery> edgeProcessorImpl = new QueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery>() {
        @Override
        public Iterator<TitanRelation> getNew(final VertexCentricQuery query) {
            InternalVertex vertex = query.getVertex();
            if (vertex.isNew() || vertex.hasAddedRelations()) {
                return (Iterator) vertex.getAddedRelations(new Predicate<InternalRelation>() {
                    //Need to filter out self-loops if query only asks for one direction

                    private TitanRelation previous = null;

                    @Override
                    public boolean apply(@Nullable InternalRelation relation) {
                        if ((relation instanceof TitanEdge) && relation.isLoop()
                                && query.getDirection() != Direction.BOTH) {
                            if (relation.equals(previous))
                                return false;

                            previous = relation;
                        }

                        return query.matches(relation);
                    }
                }).iterator();
            } else {
                return Collections.emptyIterator();
            }
        }

        @Override
        public boolean hasDeletions(VertexCentricQuery query) {
            InternalVertex vertex = query.getVertex();
            if (vertex.isNew()) return false;
            //In addition to deleted, we need to also check for added relations since those can potentially
            //replace existing ones due to a multiplicity constraint
            if (vertex.hasRemovedRelations() || vertex.hasAddedRelations()) return true;
            return false;
        }

        @Override
        public boolean isDeleted(VertexCentricQuery query, TitanRelation result) {
            if (deletedRelations.containsKey(result.longId()) || result != ((InternalRelation) result).it()) return true;
            //Check if this relation is replaced by an added one due to a multiplicity constraint
            InternalRelationType type = (InternalRelationType)result.getType();
            InternalVertex vertex = query.getVertex();
            if (type.multiplicity().isConstrained() && vertex.hasAddedRelations()) {
                final RelationComparator comparator = new RelationComparator(vertex);
                if (!Iterables.isEmpty(vertex.getAddedRelations(new Predicate<InternalRelation>() {
                    @Override
                    public boolean apply(@Nullable InternalRelation internalRelation) {
                        return comparator.compare((InternalRelation)result,internalRelation)==0;
                    }
                }))) return true;
            }
            return false;
        }

        @Override
        public Iterator<TitanRelation> execute(final VertexCentricQuery query, final SliceQuery sq, final Object exeInfo, final QueryProfiler profiler) {
            assert exeInfo==null;
            if (query.getVertex().isNew())
                return Collections.emptyIterator();

            final InternalVertex v = query.getVertex();

            EntryList iter = v.loadRelations(sq, new Retriever<SliceQuery, EntryList>() {
                @Override
                public EntryList get(SliceQuery query) {
                    return QueryProfiler.profile(profiler,query, q -> graph.edgeQuery(v.longId(), q, txHandle));
                }
            });

            return RelationConstructor.readRelation(v, iter, StandardTitanTx.this).iterator();
        }
    };

    public final QueryExecutor<GraphCentricQuery, TitanElement, JointIndexQuery> elementProcessor;

    public final QueryExecutor<GraphCentricQuery, TitanElement, JointIndexQuery> elementProcessorImpl = new QueryExecutor<GraphCentricQuery, TitanElement, JointIndexQuery>() {

        private PredicateCondition<PropertyKey, TitanElement> getEqualityCondition(Condition<TitanElement> condition) {
            if (condition instanceof PredicateCondition) {
                PredicateCondition<PropertyKey, TitanElement> pc = (PredicateCondition) condition;
                if (pc.getPredicate() == Cmp.EQUAL && TypeUtil.hasSimpleInternalVertexKeyIndex(pc.getKey())) return pc;
            } else if (condition instanceof And) {
                for (Condition<TitanElement> child : ((And<TitanElement>) condition).getChildren()) {
                    PredicateCondition<PropertyKey, TitanElement> p = getEqualityCondition(child);
                    if (p != null) return p;
                }
            }
            return null;
        }


        @Override
        public Iterator<TitanElement> getNew(final GraphCentricQuery query) {
            //If the query is unconstrained then we don't need to add new elements, so will be picked up by getVertices()/getEdges() below
            if (query.numSubQueries()==1 && query.getSubQuery(0).getBackendQuery().isEmpty())
                return Collections.emptyIterator();
            Preconditions.checkArgument(query.getCondition().hasChildren(),"If the query is non-empty it needs to have a condition");

            if (query.getResultType() == ElementCategory.VERTEX && hasModifications()) {
                Preconditions.checkArgument(QueryUtil.isQueryNormalForm(query.getCondition()));
                PredicateCondition<PropertyKey, TitanElement> standardIndexKey = getEqualityCondition(query.getCondition());
                Iterator<TitanVertex> vertices;
                if (standardIndexKey == null) {
                    final Set<PropertyKey> keys = Sets.newHashSet();
                    ConditionUtil.traversal(query.getCondition(), new Predicate<Condition<TitanElement>>() {
                        @Override
                        public boolean apply(@Nullable Condition<TitanElement> cond) {
                            Preconditions.checkArgument(cond.getType() != Condition.Type.LITERAL || cond instanceof PredicateCondition);
                            if (cond instanceof PredicateCondition)
                                keys.add(((PredicateCondition<PropertyKey, TitanElement>) cond).getKey());
                            return true;
                        }
                    });
                    Preconditions.checkArgument(!keys.isEmpty(), "Invalid query condition: %s", query.getCondition());
                    Set<TitanVertex> vertexSet = Sets.newHashSet();
                    for (TitanRelation r : addedRelations.getView(new Predicate<InternalRelation>() {
                        @Override
                        public boolean apply(@Nullable InternalRelation relation) {
                            return keys.contains(relation.getType());
                        }
                    })) {
                        vertexSet.add(((TitanVertexProperty) r).element());
                    }
                    for (TitanRelation r : deletedRelations.values()) {
                        if (keys.contains(r.getType())) {
                            TitanVertex v = ((TitanVertexProperty) r).element();
                            if (!v.isRemoved()) vertexSet.add(v);
                        }
                    }
                    vertices = vertexSet.iterator();
                } else {
                    vertices = com.google.common.collect.Iterators.transform(newVertexIndexEntries.get(standardIndexKey.getValue(), standardIndexKey.getKey()).iterator(), new Function<TitanVertexProperty, TitanVertex>() {
                        @Nullable
                        @Override
                        public TitanVertex apply(@Nullable TitanVertexProperty o) {
                            return o.element();
                        }
                    });
                }

                return (Iterator) com.google.common.collect.Iterators.filter(vertices, new Predicate<TitanVertex>() {
                    @Override
                    public boolean apply(@Nullable TitanVertex vertex) {
                        return query.matches(vertex);
                    }
                });
            } else if ( (query.getResultType() == ElementCategory.EDGE || query.getResultType()==ElementCategory.PROPERTY)
                                        && !addedRelations.isEmpty()) {
                return (Iterator) addedRelations.getView(new Predicate<InternalRelation>() {
                    @Override
                    public boolean apply(@Nullable InternalRelation relation) {
                        return query.getResultType().isInstance(relation) && !relation.isInvisible() && query.matches(relation);
                    }
                }).iterator();
            } else return Collections.emptyIterator();
        }


        @Override
        public boolean hasDeletions(GraphCentricQuery query) {
            return hasModifications();
        }

        @Override
        public boolean isDeleted(GraphCentricQuery query, TitanElement result) {
            if (result == null || result.isRemoved()) return true;
            else if (query.getResultType() == ElementCategory.VERTEX) {
                Preconditions.checkArgument(result instanceof InternalVertex);
                InternalVertex v = ((InternalVertex) result).it();
                if (v.hasAddedRelations() || v.hasRemovedRelations()) {
                    return !query.matches(result);
                } else return false;
            } else if (query.getResultType() == ElementCategory.EDGE || query.getResultType()==ElementCategory.PROPERTY) {
                Preconditions.checkArgument(result.isLoaded() || result.isNew());
                //Loaded relations are immutable so we don't need to check those
                //New relations could be modified in this transaction to now longer match the query, hence we need to
                //check for this case and consider the relations deleted
                return result.isNew() && !query.matches(result);
            } else throw new IllegalArgumentException("Unexpected type: " + query.getResultType());
        }

        @Override
        public Iterator<TitanElement> execute(final GraphCentricQuery query, final JointIndexQuery indexQuery, final Object exeInfo, final QueryProfiler profiler) {
            Iterator<TitanElement> iter;
            if (!indexQuery.isEmpty()) {
                List<QueryUtil.IndexCall<Object>> retrievals = new ArrayList<QueryUtil.IndexCall<Object>>();
                for (int i = 0; i < indexQuery.size(); i++) {
                    final JointIndexQuery.Subquery subquery = indexQuery.getQuery(i);
                    retrievals.add(new QueryUtil.IndexCall<Object>() {
                        @Override
                        public Collection<Object> call(int limit) {
                            final JointIndexQuery.Subquery adjustedQuery = subquery.updateLimit(limit);
                            try {
                                return indexCache.get(adjustedQuery, new Callable<List<Object>>() {
                                    @Override
                                    public List<Object> call() throws Exception {
                                        return QueryProfiler.profile(subquery.getProfiler(), adjustedQuery, q -> indexSerializer.query(q, txHandle));
                                    }
                                });
                            } catch (Exception e) {
                                throw new TitanException("Could not call index", e.getCause());
                            }
                        }
                    });
                }


                List<Object> resultSet = QueryUtil.processIntersectingRetrievals(retrievals, indexQuery.getLimit());
                iter = com.google.common.collect.Iterators.transform(resultSet.iterator(), getConversionFunction(query.getResultType()));
            } else {
                if (config.hasForceIndexUsage()) throw new TitanException("Could not find a suitable index to answer graph query and graph scans are disabled: " + query);
                log.warn("Query requires iterating over all vertices [{}]. For better performance, use indexes", query.getCondition());

                QueryProfiler sub = profiler.addNested("scan");
                sub.setAnnotation(QueryProfiler.QUERY_ANNOTATION,indexQuery);
                sub.setAnnotation(QueryProfiler.FULLSCAN_ANNOTATION,true);
                sub.setAnnotation(QueryProfiler.CONDITION_ANNOTATION,query.getResultType());

                switch (query.getResultType()) {
                    case VERTEX:
                        return (Iterator) getVertices().iterator();

                    case EDGE:
                        return (Iterator) getEdges().iterator();

                    case PROPERTY:
                        return new VertexCentricEdgeIterable(getInternalVertices(),RelationCategory.PROPERTY).iterator();

                    default:
                        throw new IllegalArgumentException("Unexpected type: " + query.getResultType());
                }
            }

            return iter;
        }

    };

    public Function<Object, ? extends TitanElement> getConversionFunction(final ElementCategory elementCategory) {
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

    private final Function<Object, TitanVertex> vertexIDConversionFct = new Function<Object, TitanVertex>() {
        @Override
        public TitanVertex apply(@Nullable Object id) {
            Preconditions.checkNotNull(id);
            Preconditions.checkArgument(id instanceof Long);
            return getInternalVertex((Long) id);
        }
    };

    private final Function<Object, TitanEdge> edgeIDConversionFct = new Function<Object, TitanEdge>() {
        @Override
        public TitanEdge apply(@Nullable Object id) {
            Preconditions.checkNotNull(id);
            Preconditions.checkArgument(id instanceof RelationIdentifier);
            return ((RelationIdentifier)id).findEdge(StandardTitanTx.this);
        }
    };

    private final Function<Object, TitanVertexProperty> propertyIDConversionFct = new Function<Object, TitanVertexProperty>() {
        @Override
        public TitanVertexProperty apply(@Nullable Object id) {
            Preconditions.checkNotNull(id);
            Preconditions.checkArgument(id instanceof RelationIdentifier);
            return ((RelationIdentifier)id).findProperty(StandardTitanTx.this);
        }
    };

    @Override
    public GraphCentricQueryBuilder query() {
        return new GraphCentricQueryBuilder(this, graph.getIndexSerializer());
    }

    @Override
    public TitanIndexQuery indexQuery(String indexName, String query) {
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
                throw new TitanException("Could not rollback after a failed commit", e);
            }
            throw new TitanException("Could not commit transaction due to exception during persistence", e);
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
            throw new TitanException("Could not rollback transaction due to exception", e);
        } finally {
            releaseTransaction();
            if (null != config.getGroupName() && !success) {
                MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "rollback.exceptions").inc();
            }
        }
    }

    private void releaseTransaction() {
        //TODO: release non crucial data structures to preserve memory?
        isOpen = false;
        graph.closeTransaction(this);
        vertexCache.close();
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

}
