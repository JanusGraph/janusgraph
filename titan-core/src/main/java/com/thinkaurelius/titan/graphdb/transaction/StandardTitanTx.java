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
import com.thinkaurelius.titan.core.time.Duration;
import com.thinkaurelius.titan.core.time.SimpleDuration;
import com.thinkaurelius.titan.core.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsTransaction;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPool;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
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
import com.thinkaurelius.titan.graphdb.relations.StandardProperty;
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
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanLabelVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import com.thinkaurelius.titan.graphdb.util.IndexHelper;
import com.thinkaurelius.titan.graphdb.util.VertexCentricEdgeIterable;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.lang.StringUtils;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardTitanTx extends TitanBlueprintsTransaction implements TypeInspector, VertexFactory {

    private static final Logger log = LoggerFactory.getLogger(StandardTitanTx.class);

    private static final Map<Long, InternalRelation> EMPTY_DELETED_RELATIONS = ImmutableMap.of();
    private static final ConcurrentMap<LockTuple, TransactionLock> UNINITIALIZED_LOCKS = null;
    private static final Duration LOCK_TIMEOUT = new SimpleDuration(5000L, TimeUnit.MILLISECONDS);

    private final StandardTitanGraph graph;
    private final TransactionConfiguration config;
    private final IDInspector idInspector;
    private final AttributeHandling attributeHandler;
    private final BackendTransaction txHandle;
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

    private final Retriever<Long, InternalVertex> existingVertexRetriever = new VertexConstructor(false);

    private final Retriever<Long, InternalVertex> externalVertexRetriever;
    private final Retriever<Long, InternalVertex> internalVertexRetriever;

    public StandardTitanTx(StandardTitanGraph graph, TransactionConfiguration config, BackendTransaction txHandle) {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(graph.isOpen());
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(txHandle);
        this.graph = graph;
        this.times = graph.getConfiguration().getTimestampProvider();
        this.config = config;
        this.idInspector = graph.getIDInspector();
        this.attributeHandler = graph.getDataSerializer();
        this.txHandle = txHandle;
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

        externalVertexRetriever = new VertexConstructor(config.hasVerifyExternalVertexExistence());
        internalVertexRetriever = new VertexConstructor(config.hasVerifyInternalVertexExistence());

        vertexCache = new GuavaVertexCache(config.getVertexCacheSize(),concurrencyLevel,config.getDirtyVertexSize());
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

    /*
     * ------------------------------------ Utility Access Verification methods ------------------------------------
     */

    private void verifyWriteAccess(TitanVertex... vertices) {
        if (config.isReadOnly())
            throw new UnsupportedOperationException("Cannot create new entities in read-only transaction");
        verifyAccess(vertices);
    }

    public final void verifyAccess(TitanVertex... vertices) {
        verifyOpen();
        for (TitanVertex v : vertices) {
            Preconditions.checkArgument(v instanceof InternalVertex, "Invalid vertex: %s", v);
            if (!(v instanceof SystemType) && this != ((InternalVertex) v).tx())
                throw new IllegalArgumentException("The vertex or type is not associated with this transaction [" + v + "]");
            if (v.isRemoved())
                throw new IllegalArgumentException("The vertex or type has been removed [" + v + "]");
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

    public StandardTitanGraph getGraph() {
        return graph;
    }

    public BackendTransaction getTxHandle() {
        return txHandle;
    }

    public EdgeSerializer getEdgeSerializer() {
        return edgeSerializer;
    }

    /*
     * ------------------------------------ Vertex Handling ------------------------------------
     */

    @Override
    public boolean containsVertex(final long vertexid) {
        return getVertex(vertexid) != null;
    }

    @Override
    public TitanVertex getVertex(final long vertexid) {
        verifyOpen();

        if (vertexid <= 0 || !(idInspector.isSchemaVertexId(vertexid) || idInspector.isVertexId(vertexid)))
            return null;

        if (null != config.getGroupName()) {
            MetricManager.INSTANCE.getCounter(config.getGroupName(), "db", "getVertexByID").inc();
        }

        InternalVertex v = vertexCache.get(vertexid, externalVertexRetriever);
        return (v.isRemoved()) ? null : v;
    }

    public InternalVertex getExistingVertex(long vertexid) {
        //return vertex no matter what, even if deleted, and assume the id has the correct format
        return vertexCache.get(vertexid, internalVertexRetriever);
    }

    private class VertexConstructor implements Retriever<Long, InternalVertex> {

        private final boolean verifyExistence;

        private VertexConstructor(boolean verifyExistence) {
            this.verifyExistence = verifyExistence;
        }

        @Override
        public InternalVertex get(Long vertexid) {
            Preconditions.checkNotNull(vertexid);
            Preconditions.checkArgument(vertexid > 0);
            Preconditions.checkArgument(idInspector.isSchemaVertexId(vertexid) || idInspector.isVertexId(vertexid), "Not a valid vertex id: %s", vertexid);

            byte lifecycle = ElementLifeCycle.Loaded;
            if (verifyExistence) {
                if (graph.edgeQuery(vertexid, graph.vertexExistenceQuery, txHandle).isEmpty())
                    lifecycle = ElementLifeCycle.Removed;
            }

            InternalVertex vertex = null;
            if (idInspector.isRelationTypeId(vertexid)) {
                if (idInspector.isPropertyKeyId(vertexid)) {
                    if (idInspector.isSystemRelationTypeId(vertexid)) {
                        vertex = SystemTypeManager.getSystemType(vertexid);
                    } else {
                        vertex = new TitanKeyVertex(StandardTitanTx.this, vertexid, lifecycle);
                    }
                } else {
                    assert idInspector.isEdgeLabelId(vertexid);
                    if (idInspector.isSystemRelationTypeId(vertexid)) {
                        vertex = SystemTypeManager.getSystemType(vertexid);
                    } else {
                        vertex = new TitanLabelVertex(StandardTitanTx.this, vertexid, lifecycle);
                    }
                }
            } else if (idInspector.isGenericSchemaVertexId(vertexid)) {
                vertex = new TitanSchemaVertex(StandardTitanTx.this,vertexid, lifecycle);
            } else if (idInspector.isVertexId(vertexid)) {
                vertex = new CacheVertex(StandardTitanTx.this, vertexid, lifecycle);
            } else throw new IllegalArgumentException("ID could not be recognized");
            return vertex;
        }
    }

    @Override
    public TitanVertex addVertex(Long vertexId) {
        verifyWriteAccess();
        if (vertexId != null && !graph.getConfiguration().allowVertexIdSetting()) {
            log.info("Provided vertex id [{}] is ignored because vertex id setting is not enabled", vertexId);
            vertexId = null;
        }
        Preconditions.checkArgument(vertexId != null || !graph.getConfiguration().allowVertexIdSetting(), "Must provide vertex id");
        Preconditions.checkArgument(vertexId == null || IDManager.VertexIDType.Vertex.is(vertexId), "Not a valid vertex id: %s", vertexId);
        Preconditions.checkArgument(vertexId == null || !config.hasVerifyExternalVertexExistence() || !containsVertex(vertexId), "Vertex with given id already exists: %s", vertexId);
        StandardVertex vertex = new StandardVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.Vertex, temporaryIds.nextID()), ElementLifeCycle.New);
        if (vertexId != null) {
            vertex.setID(vertexId);
        } else if (config.hasAssignIDsImmediately()) {
            graph.assignID(vertex);
        }
        addProperty(vertex, BaseKey.VertexExists, Boolean.TRUE);
        vertexCache.add(vertex, vertex.getID());
        return vertex;

    }

    @Override
    public TitanVertex addVertex() {
        return addVertex(null);
    }


    private Iterable<InternalVertex> getInternalVertices() {
        if (!addedRelations.isEmpty()) {
            //There are possible new vertices
            List<InternalVertex> newVs = vertexCache.getAllNew();
            Iterator<InternalVertex> viter = newVs.iterator();
            while (viter.hasNext()) {
                if (viter.next() instanceof TitanType) viter.remove();
            }
            return Iterables.concat(newVs, new VertexIterable(graph, this));
        } else {
            return new VertexIterable(graph, this);
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return (Iterable)getInternalVertices();
    }

    /*
     * ------------------------------------ Adding and Removing Relations ------------------------------------
     */

    public final Object verifyAttribute(TitanKey key, Object attribute) {
        Preconditions.checkNotNull(attribute, "Property value cannot be null");
        Class<?> datatype = key.getDataType();
        if (datatype.equals(Object.class)) {
            return attribute;
        } else {
            if (!attribute.getClass().equals(datatype)) {
                Object converted = attributeHandler.convert(datatype, attribute);
                Preconditions.checkArgument(converted != null,
                        "Value [%s] is not an instance of the expected data type for property key [%s] and cannot be converted. Expected: %s, found: %s", attribute,
                        key.getName(), datatype, attribute.getClass());
                attribute = converted;
            }
            Preconditions.checkState(attribute.getClass().equals(datatype));
            attributeHandler.verifyAttribute(datatype, attribute);
            return attribute;
        }
    }

    public void removeRelation(InternalRelation relation) {
        Preconditions.checkArgument(!relation.isRemoved());
        relation = relation.it();
        //Delete from Vertex
        for (int i = 0; i < relation.getLen(); i++) {
            relation.getVertex(i).removeRelation(relation);
        }
        //Update transaction data structures
        if (relation.isNew()) {
            addedRelations.remove(relation);
            if (TypeUtil.hasSimpleInternalVertexKeyIndex(relation)) newVertexIndexEntries.remove((TitanProperty) relation);
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
            deletedRelations.put(relation.getID(), relation);
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

    private TransactionLock getUniquenessLock(final TitanVertex out, final InternalType type, final Object in) {
        Multiplicity multiplicity = type.getMultiplicity();
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


    @Override
    public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, TitanLabel label) {
        verifyWriteAccess(outVertex, inVertex);
        outVertex = ((InternalVertex) outVertex).it();
        inVertex = ((InternalVertex) inVertex).it();
        Preconditions.checkNotNull(label);
        Multiplicity multiplicity = label.getMultiplicity();
        TransactionLock uniqueLock = getUniquenessLock(outVertex, (InternalType) label,inVertex);
        uniqueLock.lock(LOCK_TIMEOUT);
        try {
            //Check uniqueness
            if (config.hasVerifyUniqueness()) {
                if (multiplicity==Multiplicity.SIMPLE) {
                    Preconditions.checkArgument(Iterables.isEmpty(query(outVertex).type(label).direction(Direction.OUT).adjacentVertex(inVertex).titanEdges()),
                            "An edge with the given label already exists between the pair of vertices and the label [%s] is simple", label.getName());
                }
                if (multiplicity.isUnique(Direction.OUT)) {
                    Preconditions.checkArgument(Iterables.isEmpty(query(outVertex).type(label).direction(Direction.OUT).titanEdges()),
                            "An edge with the given label already exists on the out-vertex and the label [%s] is out-unique", label.getName());
                }
                if (multiplicity.isUnique(Direction.IN)) {
                    Preconditions.checkArgument(Iterables.isEmpty(query(inVertex).type(label).direction(Direction.IN).titanEdges()),
                            "An edge with the given label already exists on the in-vertex and the label [%s] is in-unique", label.getName());
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
        for (int pos = 0; pos < r.getLen(); pos++) vertexCache.add(r.getVertex(pos), r.getVertex(pos).getID());
        if (TypeUtil.hasSimpleInternalVertexKeyIndex(r)) newVertexIndexEntries.add((TitanProperty) r);
    }

    @Override
    public TitanProperty addProperty(TitanVertex vertex, TitanKey key, Object value) {
        if (key.getCardinality()==Cardinality.SINGLE) return setProperty(vertex, key, value);
        else return addPropertyInternal(vertex, key, value);
    }

    public TitanProperty addPropertyInternal(TitanVertex vertex, final TitanKey key, Object value) {
        verifyWriteAccess(vertex);
        Preconditions.checkArgument(!(key instanceof ImplicitKey),"Cannot create a property of implicit type: %s",key.getName());
        vertex = ((InternalVertex) vertex).it();
        Preconditions.checkNotNull(key);
        final Object normalizedValue = verifyAttribute(key, value);
        Cardinality cardinality = key.getCardinality();

        //Determine unique indexes
        List<IndexLockTuple> uniqueIndexTuples = new ArrayList<IndexLockTuple>();
        for (InternalIndexType index : TypeUtil.getUniqueIndexes(key)) {
            IndexSerializer.IndexRecords matches = IndexSerializer.indexMatches(vertex, index, key, normalizedValue);
            for (Object[] match : matches.getRecordValues()) uniqueIndexTuples.add(new IndexLockTuple(index,match));
        }

        TransactionLock uniqueLock = getUniquenessLock(vertex, (InternalType) key, normalizedValue);
        //Add locks for unique indexes
        for (IndexLockTuple lockTuple : uniqueIndexTuples) uniqueLock = new CombinerLock(uniqueLock,getLock(lockTuple),times);
        uniqueLock.lock(LOCK_TIMEOUT);
        try {
            //Check uniqueness
            if (config.hasVerifyUniqueness()) {
                if (cardinality==Cardinality.SINGLE) {
                    Preconditions.checkArgument(Iterables.isEmpty(query(vertex).type(key).properties()),
                            "A property with the given key [%s] already exists on the vertex [%s] and the property key is defined as single-valued", key.getName(), vertex);
                }
                if (cardinality==Cardinality.SET) {
                    Preconditions.checkArgument(Iterables.isEmpty(Iterables.filter(query(vertex).type(key).properties(),new Predicate<TitanProperty>() {
                        @Override
                        public boolean apply(@Nullable TitanProperty titanProperty) {
                            return normalizedValue.equals(titanProperty.getValue());
                        }
                    })),
                            "A property with the given key [%s] and value [%s] already exists on the vertex and the property key is defined as set-valued", key.getName(), normalizedValue);
                }
                //Check all unique indexes
                for (IndexLockTuple lockTuple : uniqueIndexTuples) {
                    Preconditions.checkArgument(Iterables.isEmpty(IndexHelper.getQueryResults(lockTuple.getIndex(), lockTuple.getAll(), this)),
                            "Adding this property for key [%s] and value [%s] violates a uniqueness constraint [%s]", key.getName(), normalizedValue, lockTuple.getIndex());
                }
            }
            StandardProperty prop = new StandardProperty(IDManager.getTemporaryRelationID(temporaryIds.nextID()), key, (InternalVertex) vertex, normalizedValue, ElementLifeCycle.New);
            if (config.hasAssignIDsImmediately()) graph.assignID(prop);
            connectRelation(prop);
            return prop;
        } finally {
            uniqueLock.unlock();
        }
    }

    public TitanProperty setProperty(TitanVertex vertex, final TitanKey key, Object value) {
        verifyWriteAccess(vertex);
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(key.getCardinality()==Cardinality.SINGLE, "Not an single key: %s. Use addProperty instead", key.getName());

        TransactionLock uniqueLock = FakeLock.INSTANCE;
        try {
            if (config.hasVerifyUniqueness()) {
                //Acquire uniqueness lock, remove and add
                uniqueLock = getLock(vertex, key, Direction.OUT);
                uniqueLock.lock(LOCK_TIMEOUT);
                vertex.removeProperty(key);
            } else {
                //Only delete in-memory
                InternalVertex v = (InternalVertex) vertex;
                for (InternalRelation r : v.it().getAddedRelations(new Predicate<InternalRelation>() {
                    @Override
                    public boolean apply(@Nullable InternalRelation p) {
                        return p.getType().equals(key);
                    }
                })) {
                    r.remove();
                }
            }
            return addPropertyInternal(vertex, key, value);
        } finally {
            uniqueLock.unlock();
        }
    }

    @Override
    public Iterable<Edge> getEdges() {
        return new VertexCentricEdgeIterable(getInternalVertices(),RelationCategory.EDGE);
    }



    /*
     * ------------------------------------ Type Handling ------------------------------------
     */

    public final TitanSchemaVertex makeSchemaVertex(TitanSchemaCategory typeCategory, String name, TypeDefinitionMap definition) {
        verifyOpen();
        Preconditions.checkArgument(!typeCategory.hasName() || StringUtils.isNotBlank(name), "Need to provide a valid name for type [%s]", typeCategory);
        typeCategory.verifyValidDefinition(definition);
        TitanSchemaVertex type;
        if (typeCategory.isRelationType()) {
            if (typeCategory == TitanSchemaCategory.KEY) {
                type = new TitanKeyVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.UserPropertyKey, temporaryIds.nextID()), ElementLifeCycle.New);
            } else {
                assert typeCategory == TitanSchemaCategory.LABEL;
                type = new TitanLabelVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.UserEdgeLabel,temporaryIds.nextID()), ElementLifeCycle.New);
            }
        } else {
            type = new TitanSchemaVertex(this, IDManager.getTemporaryVertexID(IDManager.VertexIDType.GenericSchemaType,temporaryIds.nextID()), ElementLifeCycle.New);
        }

        graph.assignID(type);
        Preconditions.checkArgument(type.getID() > 0);
        if (typeCategory.hasName()) addProperty(type, BaseKey.TypeName, name);
        addProperty(type, BaseKey.VertexExists, Boolean.TRUE);
        addProperty(type, BaseKey.TypeCategory, typeCategory);
        for (Map.Entry<TypeDefinitionCategory,Object> def : definition.entrySet()) {
            TitanProperty p = addProperty(type, BaseKey.TypeDefinitionProperty,def.getValue());
            p.setProperty(BaseKey.TypeDefinitionDesc,TypeDefinitionDescription.of(def.getKey()));
        }
        vertexCache.add(type, type.getID());
        if (typeCategory.hasName()) newTypeCache.put(name, type.getID());
        return type;

    }

    public TitanKey makePropertyKey(String name, TypeDefinitionMap definition) {
        return (TitanKey) makeSchemaVertex(TitanSchemaCategory.KEY, name, definition);
    }

    public TitanLabel makeEdgeLabel(String name, TypeDefinitionMap definition) {
        return (TitanLabel) makeSchemaVertex(TitanSchemaCategory.LABEL, name, definition);
    }

    @Override
    public boolean containsType(String name) {
        verifyOpen();
        return (newTypeCache.containsKey(name) || SystemTypeManager.isSystemType(name) || graph.getSchemaCache().getTypeId(name,this)!=null);
    }

    @Override
    public TitanType getType(String name) {
        verifyOpen();

        TitanType type = SystemTypeManager.getSystemType(name);
        if (type!=null) return type;

        Long typeId = newTypeCache.get(name);
        if (typeId==null) typeId=graph.getSchemaCache().getTypeId(name,this);
        if (typeId != null) {
            InternalVertex typeVertex = vertexCache.get(typeId, existingVertexRetriever);
            assert typeVertex!=null;
            return (TitanType) typeVertex;
        } else return null;
    }

    // this is critical path we can't allow anything heavier then assertion in here
    @Override
    public TitanType getExistingType(long typeid) {
        assert idInspector.isRelationTypeId(typeid);
        if (idInspector.isSystemRelationTypeId(typeid)) {
            return SystemTypeManager.getSystemType(typeid);
        } else {
            InternalVertex v = getExistingVertex(typeid);
            assert v instanceof TitanType;
            return (TitanType) v;
        }
    }

    @Override
    public TitanKey getPropertyKey(String name) {
        TitanType et = getType(name);
        if (et == null) {
            return config.getAutoEdgeTypeMaker().makeKey(makeKey(name));
        } else if (et.isPropertyKey()) {
            return (TitanKey) et;
        } else
            throw new IllegalArgumentException("The type of given name is not a key: " + name);

    }

    @Override
    public TitanLabel getEdgeLabel(String name) {
        TitanType et = getType(name);
        if (et == null) {
            return config.getAutoEdgeTypeMaker().makeLabel(makeLabel(name));
        } else if (et.isEdgeLabel()) {
            return (TitanLabel) et;
        } else
            throw new IllegalArgumentException("The type of given name is not a label: " + name);
    }

    @Override
    public KeyMaker makeKey(String name) {
        StandardKeyMaker maker = new StandardKeyMaker(this, indexSerializer, attributeHandler);
        maker.name(name);
        return maker;
    }

    @Override
    public LabelMaker makeLabel(String name) {
        StandardLabelMaker maker = new StandardLabelMaker(this, indexSerializer, attributeHandler);
        maker.name(name);
        return maker;
    }

    /*
     * ------------------------------------ Query Answering ------------------------------------
     */

    public VertexCentricQueryBuilder query(TitanVertex vertex) {
        return new VertexCentricQueryBuilder((InternalVertex) vertex);
    }

    @Override
    public TitanMultiVertexQuery multiQuery(TitanVertex... vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this);
        for (TitanVertex v : vertices) builder.addVertex(v);
        return builder;
    }

    @Override
    public TitanMultiVertexQuery multiQuery(Collection<TitanVertex> vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this);
        builder.addAllVertices(vertices);
        return builder;
    }

    public void executeMultiQuery(final Collection<InternalVertex> vertices, final SliceQuery sq) {
        LongArrayList vids = new LongArrayList(vertices.size());
        for (InternalVertex v : vertices) {
            if (!v.isNew() && v.hasId() && (v instanceof CacheVertex) && !v.hasLoadedRelations(sq)) vids.add(v.getID());
        }

        if (!vids.isEmpty()) {
            List<EntryList> results = graph.edgeMultiQuery(vids, sq, txHandle);
            int pos = 0;
            for (TitanVertex v : vertices) {
                if (pos<vids.size() && vids.get(pos) == v.getID()) {
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
                return Iterators.emptyIterator();
            }
        }

        @Override
        public boolean hasDeletions(VertexCentricQuery query) {
            return !deletedRelations.isEmpty() && !query.getVertex().isNew() && query.getVertex().hasRemovedRelations();
        }

        @Override
        public boolean isDeleted(VertexCentricQuery query, TitanRelation result) {
            return deletedRelations.containsKey(result.getID()) || result != ((InternalRelation) result).it();
        }

        @Override
        public Iterator<TitanRelation> execute(final VertexCentricQuery query, final SliceQuery sq, final Object exeInfo) {
            assert exeInfo==null;
            if (query.getVertex().isNew())
                return Iterators.emptyIterator();

            final InternalVertex v = query.getVertex();

            Iterable<Entry> iter = v.loadRelations(sq, new Retriever<SliceQuery, EntryList>() {
                @Override
                public EntryList get(SliceQuery query) {
                    return graph.edgeQuery(v.getID(), query, txHandle);
                }
            });

            return RelationConstructor.readRelation(v, iter, StandardTitanTx.this).iterator();
        }
    };

    public final QueryExecutor<GraphCentricQuery, TitanElement, JointIndexQuery> elementProcessor;

    public final QueryExecutor<GraphCentricQuery, TitanElement, JointIndexQuery> elementProcessorImpl = new QueryExecutor<GraphCentricQuery, TitanElement, JointIndexQuery>() {

        private PredicateCondition<TitanKey, TitanElement> getEqualityCondition(Condition<TitanElement> condition) {
            if (condition instanceof PredicateCondition) {
                PredicateCondition<TitanKey, TitanElement> pc = (PredicateCondition) condition;
                if (pc.getPredicate() == Cmp.EQUAL && TypeUtil.hasSimpleInternalVertexKeyIndex(pc.getKey())) return pc;
            } else if (condition instanceof And) {
                for (Condition<TitanElement> child : ((And<TitanElement>) condition).getChildren()) {
                    PredicateCondition<TitanKey, TitanElement> p = getEqualityCondition(child);
                    if (p != null) return p;
                }
            }
            return null;
        }


        @Override
        public Iterator<TitanElement> getNew(final GraphCentricQuery query) {
            Preconditions.checkArgument(query.getResultType() == ElementCategory.VERTEX || query.getResultType() == ElementCategory.EDGE);
            //If the query is unconstrained then we don't need to add new elements, so will be picked up by getVertices()/getEdges() below
            if (query.numSubQueries()==1 && query.getSubQuery(0).getBackendQuery().isEmpty()) return Iterators.emptyIterator();
            Preconditions.checkArgument(query.getCondition().hasChildren(),"If the query is non-empty it needs to have a condition");

            if (query.getResultType() == ElementCategory.VERTEX && hasModifications()) {
                Preconditions.checkArgument(QueryUtil.isQueryNormalForm(query.getCondition()));
                PredicateCondition<TitanKey, TitanElement> standardIndexKey = getEqualityCondition(query.getCondition());
                Iterator<TitanVertex> vertices;
                if (standardIndexKey == null) {
                    final Set<TitanKey> keys = Sets.newHashSet();
                    ConditionUtil.traversal(query.getCondition(), new Predicate<Condition<TitanElement>>() {
                        @Override
                        public boolean apply(@Nullable Condition<TitanElement> cond) {
                            Preconditions.checkArgument(cond.getType() != Condition.Type.LITERAL || cond instanceof PredicateCondition);
                            if (cond instanceof PredicateCondition)
                                keys.add(((PredicateCondition<TitanKey, TitanElement>) cond).getKey());
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
                        vertexSet.add(((TitanProperty) r).getVertex());
                    }
                    for (TitanRelation r : deletedRelations.values()) {
                        if (keys.contains(r.getType())) {
                            TitanVertex v = ((TitanProperty) r).getVertex();
                            if (!v.isRemoved()) vertexSet.add(v);
                        }
                    }
                    vertices = vertexSet.iterator();
                } else {
                    vertices = Iterators.transform(newVertexIndexEntries.get(standardIndexKey.getValue(), standardIndexKey.getKey()).iterator(), new Function<TitanProperty, TitanVertex>() {
                        @Nullable
                        @Override
                        public TitanVertex apply(@Nullable TitanProperty o) {
                            return o.getVertex();
                        }
                    });
                }


                return (Iterator) Iterators.filter(vertices, new Predicate<TitanVertex>() {
                    @Override
                    public boolean apply(@Nullable TitanVertex vertex) {
                        return query.matches(vertex);
                    }
                });
            } else if (query.getResultType() == ElementCategory.EDGE && !addedRelations.isEmpty()) {
                return (Iterator) addedRelations.getView(new Predicate<InternalRelation>() {
                    @Override
                    public boolean apply(@Nullable InternalRelation relation) {
                        return (relation instanceof TitanEdge) && !relation.isHidden() && query.matches(relation);
                    }
                }).iterator();
            } else return Iterators.emptyIterator();
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
            } else if (query.getResultType() == ElementCategory.EDGE) {
                //Loaded edges are immutable and new edges are previously filtered
                Preconditions.checkArgument(result.isLoaded() || result.isNew());
                return false;
            } else throw new IllegalArgumentException("Unexpected type: " + query.getResultType());
        }

        @Override
        public Iterator<TitanElement> execute(final GraphCentricQuery query, final JointIndexQuery indexQuery, final Object exeInfo) {
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
                                        return indexSerializer.query(adjustedQuery, txHandle);
                                    }
                                });
                            } catch (Exception e) {
                                throw new TitanException("Could not call index", e.getCause());
                            }
                        }
                    });
                }


                List<Object> resultSet = QueryUtil.processIntersectingRetrievals(retrievals, indexQuery.getLimit());
                iter = Iterators.transform(resultSet.iterator(), getConversionFunction(query.getResultType()));
            } else {
                log.warn("Query requires iterating over all vertices [{}]. For better performance, use indexes", query.getCondition());

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
            default:
                throw new IllegalArgumentException("Unexpected result type: " + elementCategory);
        }
    }

    private final Function<Object, TitanVertex> vertexIDConversionFct = new Function<Object, TitanVertex>() {
        @Override
        public TitanVertex apply(@Nullable Object id) {
            Preconditions.checkNotNull(id);
            Preconditions.checkArgument(id instanceof Long);
            return getExistingVertex((Long) id);
        }
    };

    private final Function<Object, TitanEdge> edgeIDConversionFct = new Function<Object, TitanEdge>() {
        @Override
        public TitanEdge apply(@Nullable Object id) {
            Preconditions.checkNotNull(id);
            Preconditions.checkArgument(id instanceof RelationIdentifier);
            return getEdge(id);
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

    @Override
    public Iterable<TitanVertex> getVertices(TitanKey key, Object attribute) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(attribute);
        return (Iterable) query().has(key, Cmp.EQUAL, attribute).vertices();
    }

    @Override
    public TitanVertex getVertex(TitanKey key, Object attribute) {
        return Iterables.getOnlyElement(getVertices(key, attribute), null);
    }

    @Override
    public TitanVertex getVertex(String key, Object attribute) {
        if (!containsType(key)) return null;
        else return getVertex(getPropertyKey(key), attribute);
    }

    @Override
    public Iterable<TitanEdge> getEdges(TitanKey key, Object attribute) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(attribute);
        return (Iterable) query().has(key, Cmp.EQUAL, attribute).edges();
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
            } catch (StorageException e1) {
                throw new TitanException("Could not rollback after a failed commit", e);
            }
            throw new TitanException("Could not commit transaction due to exception during persistence", e);
        } finally {
            close();
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
            close();
            if (null != config.getGroupName() && !success) {
                MetricManager.INSTANCE.getCounter(config.getGroupName(), "tx", "rollback.exceptions").inc();
            }
        }
    }

    private void close() {
        //TODO: release non crucial data structures to preserve memory?
        isOpen = false;
        graph.closeTransaction(this);
        vertexCache.close();
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isClosed() {
        return !isOpen;
    }

    @Override
    public boolean hasModifications() {
        return !addedRelations.isEmpty() || !deletedRelations.isEmpty();
    }

}
