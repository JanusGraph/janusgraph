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
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsTransaction;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.*;
import com.thinkaurelius.titan.graphdb.query.*;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.relations.StandardEdge;
import com.thinkaurelius.titan.graphdb.relations.StandardProperty;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.AddedRelationsContainer;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.ConcurrentBufferAddedRelations;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.SimpleBufferAddedRelations;
import com.thinkaurelius.titan.graphdb.transaction.indexcache.ConcurrentIndexCache;
import com.thinkaurelius.titan.graphdb.transaction.indexcache.IndexCache;
import com.thinkaurelius.titan.graphdb.transaction.indexcache.SimpleIndexCache;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.LRUVertexCache;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.VertexCache;
import com.thinkaurelius.titan.graphdb.types.StandardKeyMaker;
import com.thinkaurelius.titan.graphdb.types.StandardLabelMaker;
import com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import com.thinkaurelius.titan.graphdb.types.TypeAttribute;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanLabelVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanTypeVertex;
import com.thinkaurelius.titan.graphdb.util.FakeLock;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardTitanTx extends TitanBlueprintsTransaction {

    private static final Logger log = LoggerFactory.getLogger(StandardTitanTx.class);

    private static final Map<Long, InternalRelation> EMPTY_DELETED_RELATIONS = ImmutableMap.of();
    private static final ConcurrentMap<UniqueLockApplication, Lock> UNINITIALIZED_LOCKS = null;

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
    private final Cache<IndexQuery, List<Object>> indexCache;
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
    private ConcurrentMap<UniqueLockApplication, Lock> uniqueLocks;

    //####### Other Data structures
    /**
     * Caches Titan types by name so that they can be quickly retrieved once they are loaded in the transaction.
     * Since type retrieval by name is common and there are only a few types, since cache is a simple map (i.e. no release)
     */
    private final Map<String, Long> typeCache;

    /**
     * Used to assign temporary ids to new vertices and relations added in this transaction.
     * If ids are assigned immediately, this is not used.
     */
    private final AtomicLong temporaryID;


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
        this.config = config;
        this.idInspector = graph.getIDInspector();
        this.attributeHandler = graph.getAttributeHandling();
        this.txHandle = txHandle;
        this.edgeSerializer = graph.getEdgeSerializer();
        this.indexSerializer = graph.getIndexSerializer();

        temporaryID = new AtomicLong(-1);

        int concurrencyLevel;
        if (config.isSingleThreaded()) {
            addedRelations = new SimpleBufferAddedRelations();
            concurrencyLevel = 1;
            typeCache = new HashMap<String, Long>();
            newVertexIndexEntries = new SimpleIndexCache();
        } else {
            addedRelations = new ConcurrentBufferAddedRelations();
            concurrencyLevel = 4;
            typeCache = new NonBlockingHashMap<String, Long>();
            newVertexIndexEntries = new ConcurrentIndexCache();
        }

        externalVertexRetriever = new VertexConstructor(config.hasVerifyExternalVertexExistence());
        internalVertexRetriever = new VertexConstructor(config.hasVerifyInternalVertexExistence());

        vertexCache = new LRUVertexCache(config.getVertexCacheSize());
        indexCache = CacheBuilder.newBuilder().weigher(new Weigher<IndexQuery, List<Object>>() {
            @Override
            public int weigh(IndexQuery q, List<Object> r) {
                return 2 + r.size();
            }
        }).concurrencyLevel(concurrencyLevel).maximumWeight(config.getIndexCacheWeight()).build();

        uniqueLocks = UNINITIALIZED_LOCKS;
        deletedRelations = EMPTY_DELETED_RELATIONS;

        this.isOpen = true;
        if (null != config.getMetricsPrefix()) {
            MetricManager.INSTANCE.getCounter(config.getMetricsPrefix(), "tx", "begin").inc();
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

        if (vertexid <= 0 || !(idInspector.isTypeID(vertexid) || idInspector.isVertexID(vertexid)))
            return null;

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
            Preconditions.checkArgument(idInspector.isTypeID(vertexid) || idInspector.isVertexID(vertexid), "Not a valid vertex id: %s", vertexid);

            byte lifecycle = ElementLifeCycle.Loaded;
            if (verifyExistence) {
                if (graph.edgeQuery(vertexid, graph.vertexExistenceQuery, txHandle).isEmpty())
                    lifecycle = ElementLifeCycle.Removed;
            }

            InternalVertex vertex = null;
            if (idInspector.isTypeID(vertexid)) {
                if (idInspector.isPropertyKeyID(vertexid)) {
                    vertex = new TitanKeyVertex(StandardTitanTx.this, vertexid, lifecycle);
                } else {
                    Preconditions.checkArgument(idInspector.isEdgeLabelID(vertexid));
                    vertex = new TitanLabelVertex(StandardTitanTx.this, vertexid, lifecycle);
                }
                //If its a newly created type, add to type cache
                if (lifecycle == ElementLifeCycle.Loaded)
                    typeCache.put(((TitanType) vertex).getName(), vertexid);
            } else if (idInspector.isVertexID(vertexid)) {
                vertex = new CacheVertex(StandardTitanTx.this, vertexid, lifecycle);
            } else throw new IllegalArgumentException("ID could not be recognized");
            return vertex;
        }
    }

    @Override
    public TitanVertex addVertex(Long vertexId) {
        verifyWriteAccess();
        if (vertexId != null && !graph.getConfiguration().allowVertexIdSetting()) {
            log.warn("Provided vertex id [{}] is ignored because vertex id setting is not enabled", vertexId);
            vertexId = null;
        }
        Preconditions.checkArgument(vertexId != null || !graph.getConfiguration().allowVertexIdSetting(), "Must provide vertex id");
        Preconditions.checkArgument(vertexId == null || IDManager.isVertexID(vertexId), "Not a valid vertex id: %s", vertexId);
        Preconditions.checkArgument(vertexId == null || !config.hasVerifyExternalVertexExistence() || !containsVertex(vertexId), "Vertex with given id already exists: %s", vertexId);
        StandardVertex vertex = new StandardVertex(this, temporaryID.decrementAndGet(), ElementLifeCycle.New);
        if (vertexId != null) {
            vertex.setID(vertexId);
        } else if (config.hasAssignIDsImmediately()) {
            graph.assignID(vertex);
        }
        addProperty(vertex, SystemKey.VertexState, SystemKey.VertexStates.DEFAULT.getValue());
        vertexCache.add(vertex, vertex.getID());
        return vertex;

    }

    @Override
    public TitanVertex addVertex() {
        return addVertex(null);
    }


    @Override
    public Iterable<Vertex> getVertices() {
        if (!addedRelations.isEmpty()) {
            //There are possible new vertices
            List<InternalVertex> newVs = vertexCache.getAllNew();
            Iterator<InternalVertex> viter = newVs.iterator();
            while (viter.hasNext()) {
                if (viter.next() instanceof TitanType) viter.remove();
            }
            return Iterables.concat((List) newVs, new VertexIterable(graph, this));
        } else {
            return (Iterable) new VertexIterable(graph, this);
        }
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

    private static final boolean isVertexIndexProperty(InternalRelation relation) {
        if (!(relation instanceof TitanProperty)) return false;
        return isVertexIndexProperty(((TitanProperty) relation).getPropertyKey());
    }

    private static final boolean isVertexIndexProperty(TitanKey key) {
        return key.hasIndex(Titan.Token.STANDARD_INDEX, Vertex.class);
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
            if (isVertexIndexProperty(relation)) newVertexIndexEntries.remove((TitanProperty) relation);
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

    private Lock getUniquenessLock(final TitanVertex start, final TitanType type, final Object end) {
        if (config.isSingleThreaded()) return FakeLock.INSTANCE;
        if (uniqueLocks == UNINITIALIZED_LOCKS) {
            Preconditions.checkArgument(!config.isSingleThreaded());
            synchronized (this) {
                if (uniqueLocks == UNINITIALIZED_LOCKS)
                    uniqueLocks = new ConcurrentHashMap<UniqueLockApplication, Lock>();
            }
        }
        UniqueLockApplication la = new UniqueLockApplication(start, type, end);
        Lock lock = new ReentrantLock();
        Lock existingLock = uniqueLocks.putIfAbsent(la, lock);
        if (existingLock == null) return lock;
        else return existingLock;
    }


    @Override
    public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, TitanLabel label) {
        verifyWriteAccess(outVertex, inVertex);
        outVertex = ((InternalVertex) outVertex).it();
        inVertex = ((InternalVertex) inVertex).it();
        Preconditions.checkNotNull(label);
        Lock uniqueLock = FakeLock.INSTANCE;
        if (config.hasVerifyUniqueness() && (label.isUnique(Direction.OUT) || label.isUnique(Direction.IN)))
            uniqueLock = getUniquenessLock(outVertex, label, inVertex);
        uniqueLock.lock();
        try {
            //Check uniqueness
            if (config.hasVerifyUniqueness()) {
                if (label.isUnique(Direction.OUT)) {
                    Preconditions.checkArgument(Iterables.isEmpty(query(outVertex).includeHidden().type(label).direction(Direction.OUT).titanEdges()),
                            "An edge with the given type already exists on the out-vertex and the label [%s] is out-unique", label.getName());
                }
                if (label.isUnique(Direction.IN)) {
                    Preconditions.checkArgument(Iterables.isEmpty(query(inVertex).includeHidden().type(label).direction(Direction.IN).titanEdges()),
                            "An edge with the given type already exists on the in-vertex and the label [%s] is in-unique", label.getName());
                }
            }
            StandardEdge edge = new StandardEdge(temporaryID.decrementAndGet(), label, (InternalVertex) outVertex, (InternalVertex) inVertex, ElementLifeCycle.New);
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
        if (isVertexIndexProperty(r)) newVertexIndexEntries.add((TitanProperty) r);
    }

    @Override
    public TitanProperty addProperty(TitanVertex vertex, TitanKey key, Object value) {
        if (key.isUnique(Direction.OUT)) return setProperty(vertex, key, value);
        else return addPropertyInternal(vertex, key, value);
    }

    public TitanProperty addPropertyInternal(TitanVertex vertex, TitanKey key, Object value) {
        verifyWriteAccess(vertex);
        vertex = ((InternalVertex) vertex).it();
        Preconditions.checkNotNull(key);
        value = verifyAttribute(key, value);
        Lock uniqueLock = FakeLock.INSTANCE;
        if (config.hasVerifyUniqueness() && (key.isUnique(Direction.OUT) || key.isUnique(Direction.IN)))
            uniqueLock = getUniquenessLock(vertex, key, value);
        uniqueLock.lock();
        try {
            //Check uniqueness
            if (config.hasVerifyUniqueness()) {
                if (key.isUnique(Direction.OUT)) {
                    Preconditions.checkArgument(Iterables.isEmpty(query(vertex).includeHidden().type(key).direction(Direction.OUT).properties()),
                            "A property with the given key [%s] already exists on the vertex [%s] and the property key is defined as single-valued", key.getName(), vertex);
                }
                if (key.isUnique(Direction.IN)) {
                    Preconditions.checkArgument(Iterables.isEmpty(getVertices(key, value)),
                            "The given value [%s] is already used as a property and the property key [%s] is defined as graph-unique", value, key.getName());
                }
            }
            StandardProperty prop = new StandardProperty(temporaryID.decrementAndGet(), key, (InternalVertex) vertex, value, ElementLifeCycle.New);
            if (config.hasAssignIDsImmediately()) graph.assignID(prop);
            connectRelation(prop);
            return prop;
        } finally {
            uniqueLock.unlock();
        }
    }

    public TitanProperty setProperty(TitanVertex vertex, final TitanKey key, Object value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(key.isUnique(Direction.OUT), "Not an out-unique key: %s", key.getName());

        Lock uniqueLock = FakeLock.INSTANCE;
        try {
            if (config.hasVerifyUniqueness()) {
                //Acquire uniqueness lock, remove and add
                uniqueLock = getUniquenessLock(vertex, key, value);
                uniqueLock.lock();
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
        return new VertexCentricEdgeIterable(getVertices());
    }



    /*
     * ------------------------------------ Type Handling ------------------------------------
     */

    private final TitanType makeTitanType(TitanTypeClass typeClass, String name, TypeAttribute.Map definition) {
        verifyOpen();
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        TitanTypeVertex type;
        if (typeClass == TitanTypeClass.KEY) {
            TypeAttribute.isValidKeyDefinition(definition);
            type = new TitanKeyVertex(this, temporaryID.decrementAndGet(), ElementLifeCycle.New);
        } else {
            Preconditions.checkArgument(typeClass == TitanTypeClass.LABEL);
            TypeAttribute.isValidLabelDefinition(definition);
            type = new TitanLabelVertex(this, temporaryID.decrementAndGet(), ElementLifeCycle.New);
        }
        graph.assignID(type);
        addProperty(type, SystemKey.VertexState, SystemKey.VertexStates.DEFAULT.getValue());
        addProperty(type, SystemKey.TypeName, name);
        addProperty(type, SystemKey.TypeClass, typeClass);
        for (TypeAttribute attribute : definition.getAttributes()) {
            addProperty(type, SystemKey.TypeDefinition, attribute);
        }
        Preconditions.checkArgument(type.getID() > 0);
        vertexCache.add(type, type.getID());
        typeCache.put(name, type.getID());
        return type;

    }

    public TitanKey makePropertyKey(String name, TypeAttribute.Map definition) {
        return (TitanKey) makeTitanType(TitanTypeClass.KEY, name, definition);
    }

    public TitanLabel makeEdgeLabel(String name, TypeAttribute.Map definition) {
        return (TitanLabel) makeTitanType(TitanTypeClass.LABEL, name, definition);
    }

    @Override
    public boolean containsType(String name) {
        verifyOpen();
        return (typeCache.containsKey(name) || SystemKey.KEY_MAP.containsKey(name) || !Iterables.isEmpty(getVertices(SystemKey.TypeName, name)));
    }

    @Override
    public TitanType getType(String name) {
        verifyOpen();

        Long typeId = typeCache.get(name);
        if (typeId != null) {
            InternalVertex typeVertex = vertexCache.get(typeId, existingVertexRetriever);
            if (typeVertex != null)
                return (TitanType) typeVertex;
        }

        TitanType type = SystemKey.KEY_MAP.get(name);
        return (type != null)
                ? type
                : (TitanType) Iterables.getOnlyElement(getVertices(SystemKey.TypeName, name), null);
    }

    // this is critical path we can't allow anything heavier then assertion in here
    public TitanType getExistingType(long typeid) {
        assert idInspector.isTypeID(typeid);

        if (SystemTypeManager.isSystemRelationType(typeid))
            return SystemTypeManager.getSystemRelationType(typeid);

        InternalVertex v = getExistingVertex(typeid);
        assert v instanceof TitanType;

        return (TitanType) v;
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
        StandardKeyMaker maker = new StandardKeyMaker(this, indexSerializer);
        maker.name(name);
        return maker;
    }

    @Override
    public LabelMaker makeLabel(String name) {
        StandardLabelMaker maker = new StandardLabelMaker(this, indexSerializer);
        maker.name(name);
        return maker;
    }

    /*
     * ------------------------------------ Query Answering ------------------------------------
     */

    public VertexCentricQueryBuilder query(TitanVertex vertex) {
        return new VertexCentricQueryBuilder((InternalVertex) vertex, edgeSerializer);
    }

    @Override
    public TitanMultiVertexQuery multiQuery(TitanVertex... vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this, edgeSerializer);
        for (TitanVertex v : vertices) builder.addVertex(v);
        return builder;
    }

    @Override
    public TitanMultiVertexQuery multiQuery(Collection<TitanVertex> vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this, edgeSerializer);
        builder.addAllVertices(vertices);
        return builder;
    }

    public void executeMultiQuery(final Collection<InternalVertex> vertices, final SliceQuery sq) {
        LongArrayList vids = new LongArrayList(vertices.size());
        for (InternalVertex v : vertices) {
            if (!v.isNew() && v.hasId() && (v instanceof CacheVertex) && !v.hasLoadedRelations(sq)) vids.add(v.getID());
        }

        if (!vids.isEmpty()) {
            List<List<Entry>> results = graph.edgeMultiQuery(vids, sq, txHandle);
            int pos = 0;
            for (TitanVertex v : vertices) {
                if (vids.get(pos) == v.getID()) {
                    final List<Entry> vresults = results.get(pos);
                    ((CacheVertex) v).loadRelations(sq, new Retriever<SliceQuery, List<Entry>>() {
                        @Override
                        public List<Entry> get(SliceQuery query) {
                            return vresults;
                        }
                    });
                }
                pos++;
            }
        }
    }

    public final QueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery> edgeProcessor = new QueryExecutor<VertexCentricQuery, TitanRelation, SliceQuery>() {
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
            if (query.getVertex().isNew())
                return Iterators.emptyIterator();

            final boolean filterDirection = (exeInfo != null) ? (Boolean) exeInfo : false;
            final InternalVertex v = query.getVertex();

            Iterable<Entry> iter = v.loadRelations(sq, new Retriever<SliceQuery, List<Entry>>() {
                @Override
                public List<Entry> get(SliceQuery query) {
                    return graph.edgeQuery(v.getID(), query, txHandle);
                }
            });

            if (filterDirection) {
                assert query.getDirection() != Direction.BOTH;
                iter = Iterables.filter(iter, new Predicate<Entry>() {
                    @Override
                    public boolean apply(@Nullable Entry entry) {
                        return edgeSerializer.parseDirection(entry) == query.getDirection();
                    }
                });
            }

            return Iterables.transform(iter, new Function<Entry, TitanRelation>() {
                @Override
                public TitanRelation apply(@Nullable Entry entry) {
                    return edgeSerializer.readRelation(v, entry);
                }
            }).iterator();
        }
    };


    public final QueryExecutor<GraphCentricQuery, TitanElement, JointIndexQuery> elementProcessor = new QueryExecutor<GraphCentricQuery, TitanElement, JointIndexQuery>() {

        private PredicateCondition<TitanKey, TitanElement> getEqualityCondition(Condition<TitanElement> condition) {
            if (condition instanceof PredicateCondition) {
                PredicateCondition<TitanKey, TitanElement> pc = (PredicateCondition) condition;
                if (pc.getPredicate() == Cmp.EQUAL && isVertexIndexProperty(pc.getKey())) return pc;
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
            Preconditions.checkArgument(query.getResultType() == ElementType.VERTEX || query.getResultType() == ElementType.EDGE);
            if (query.getResultType() == ElementType.VERTEX && hasModifications()) {
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
            } else if (query.getResultType() == ElementType.EDGE && !addedRelations.isEmpty()) {
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
            else if (query.getResultType() == ElementType.VERTEX) {
                Preconditions.checkArgument(result instanceof InternalVertex);
                InternalVertex v = ((InternalVertex) result).it();
                if (v.hasAddedRelations() || v.hasRemovedRelations()) {
                    return !query.matches(result);
                } else return false;
            } else if (query.getResultType() == ElementType.EDGE) {
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
                    final String index = indexQuery.getIndex(i);
                    final IndexQuery subquery = indexQuery.getQuery(i);
                    retrievals.add(new QueryUtil.IndexCall<Object>() {
                        @Override
                        public Collection<Object> call(int limit) {
                            final IndexQuery adjustedQuery = subquery.updateLimit(limit);
                            try {
                                return indexCache.get(adjustedQuery, new Callable<List<Object>>() {
                                    @Override
                                    public List<Object> call() throws Exception {
                                        return indexSerializer.query(index, adjustedQuery, txHandle);
                                    }
                                });
                            } catch (Exception e) {
                                throw new TitanException("Could not call index", e.getCause());
                            }
                        }
                    });
                }


                List<Object> resultSet = QueryUtil.processIntersectingRetrievals(retrievals, indexQuery.getLimit());
                iter = Iterators.transform(resultSet.iterator(), new Function<Object, TitanElement>() {
                    @Override
                    public TitanElement apply(@Nullable Object id) {
                        Preconditions.checkNotNull(id);

                        switch (query.getResultType()) {
                            case VERTEX:
                                return getExistingVertex((Long) id);

                            case EDGE:
                                return (TitanElement) getEdge(id);

                            default:
                                throw new IllegalArgumentException("Unexpected id type: " + id);
                        }
                    }
                });
            } else {
                log.warn("Query requires iterating over all vertices [{}]. For better performance, use indexes", query.getCondition());

                switch (query.getResultType()) {
                    case VERTEX:
                        return (Iterator) getVertices().iterator();

                    case EDGE:
                        return (Iterator) getEdges().iterator();

                    default:
                        throw new IllegalArgumentException("Unexpected type: " + query.getResultType());
                }
            }

            return iter;
        }

    };

    @Override
    public GraphCentricQueryBuilder query() {
        return new GraphCentricQueryBuilder(this, graph.getIndexSerializer());
    }

    @Override
    public Iterable<TitanVertex> getVertices(TitanKey key, Object attribute) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(attribute);
        return (Iterable) query().has(key, Cmp.EQUAL, attribute).vertices();
    }

    @Override
    public TitanVertex getVertex(TitanKey key, Object attribute) {
        Preconditions.checkArgument(key.isUnique(Direction.IN), "Key is not uniquely associated to value [%s]", key.getName());
        return Iterables.getOnlyElement(getVertices(key, attribute), null);
    }

    @Override
    public TitanVertex getVertex(String key, Object attribute) {
        if (!containsType(key)) return null;
        else return getVertex((TitanKey) getType(key), attribute);
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
        try {
            if (hasModifications()) {
                graph.save(addedRelations.getAll(), deletedRelations.values(), this);
            }
            txHandle.commit();
        } catch (Exception e) {
            try {
                txHandle.rollback();
            } catch (StorageException e1) {
                throw new TitanException("Could not rollback after a failed commit", e);
            }
            throw new TitanException("Could not commit transaction due to exception during persistence", e);
        } finally {
            close();
            if (null != config.getMetricsPrefix()) {
                MetricManager.INSTANCE.getCounter(config.getMetricsPrefix(), "tx", "commit").inc();
            }
        }
    }

    @Override
    public synchronized void rollback() {
        Preconditions.checkArgument(isOpen(), "The transaction has already been closed");
        try {
            txHandle.rollback();
        } catch (Exception e) {
            throw new TitanException("Could not rollback transaction due to exception", e);
        } finally {
            close();
            if (null != config.getMetricsPrefix()) {
                MetricManager.INSTANCE.getCounter(config.getMetricsPrefix(), "tx", "rollback").inc();
            }
        }
    }

    private void close() {
        //TODO: release non crucial data structures to preserve memory?
        isOpen = false;
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
