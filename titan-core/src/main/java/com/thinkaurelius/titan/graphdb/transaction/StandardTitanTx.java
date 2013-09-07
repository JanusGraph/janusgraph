package com.thinkaurelius.titan.graphdb.transaction;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsTransaction;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.*;
import com.thinkaurelius.titan.graphdb.query.condition.*;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.graphdb.relations.StandardEdge;
import com.thinkaurelius.titan.graphdb.relations.StandardProperty;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.AddedRelationsContainer;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.ConcurrentBufferAddedRelations;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.SimpleBufferAddedRelations;
import com.thinkaurelius.titan.graphdb.transaction.indexcache.ConcurrentIndexCache;
import com.thinkaurelius.titan.graphdb.transaction.indexcache.IndexCache;
import com.thinkaurelius.titan.graphdb.transaction.indexcache.SimpleIndexCache;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.ConcurrentVertexCache;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.SimpleVertexCache;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.VertexCache;
import com.thinkaurelius.titan.graphdb.types.StandardTypeMaker;
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
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    private static final long DEFAULT_CACHE_SIZE = 10000;

    private final StandardTitanGraph graph;
    private final TransactionConfig config;
    private final IDInspector idInspector;
    private final AttributeHandling attributeHandler;
    private final BackendTransaction txHandle;

    //Internal data structures
    private final VertexCache vertexCache;
    private final AtomicLong temporaryID;
    private final AddedRelationsContainer addedRelations;
    private Map<Long, InternalRelation> deletedRelations;
    private final Cache<IndexQuery, List<Object>> indexCache;
    private final IndexCache newVertexIndexEntries;
    private ConcurrentMap<UniqueLockApplication, Lock> uniqueLocks;

    private final Map<String, TitanType> typeCache;

    private boolean isOpen;


    public StandardTitanTx(StandardTitanGraph graph, TransactionConfig config, BackendTransaction txHandle) {
        Preconditions.checkNotNull(graph);
        Preconditions.checkArgument(graph.isOpen());
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(txHandle);
        this.graph = graph;
        this.config = config;
        this.idInspector = graph.getIDInspector();
        this.attributeHandler = graph.getAttributeHandling();
        this.txHandle = txHandle;

        temporaryID = new AtomicLong(-1);
        Cache<GraphCentricQuery, List<Object>> indexCacheBuilder = CacheBuilder.newBuilder().weigher(new Weigher<GraphCentricQuery, List<Object>>() {
            @Override
            public int weigh(GraphCentricQuery q, List<Object> r) {
                return 2 + r.size();
            }
        }).
                maximumWeight(DEFAULT_CACHE_SIZE).build();
        int concurrencyLevel;
        if (config.isSingleThreaded()) {
            vertexCache = new SimpleVertexCache();
            addedRelations = new SimpleBufferAddedRelations();
            concurrencyLevel = 1;
            typeCache = new HashMap<String, TitanType>();
            newVertexIndexEntries = new SimpleIndexCache();
        } else {
            vertexCache = new ConcurrentVertexCache();
            addedRelations = new ConcurrentBufferAddedRelations();
            concurrencyLevel = 4;
            typeCache = new ConcurrentHashMap<String, TitanType>();
            newVertexIndexEntries = new ConcurrentIndexCache();
        }
        for (SystemType st : SystemKey.values()) typeCache.put(st.getName(), st);

        indexCache = CacheBuilder.newBuilder().weigher(new Weigher<IndexQuery, List<Object>>() {
            @Override
            public int weigh(IndexQuery q, List<Object> r) {
                return 2 + r.size();
            }
        }).concurrencyLevel(concurrencyLevel).maximumWeight(DEFAULT_CACHE_SIZE).build();

        uniqueLocks = UNINITIALIZED_LOCKS;
        deletedRelations = EMPTY_DELETED_RELATIONS;
        this.isOpen = true;
    }

    /*
     * ------------------------------------ Utility Access Verification methods ------------------------------------
     */

    private final void verifyWriteAccess(TitanVertex... vertices) {
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

    private final void verifyOpen() {
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

    public TransactionConfig getConfiguration() {
        return config;
    }

    public StandardTitanGraph getGraph() {
        return graph;
    }

    public BackendTransaction getTxHandle() {
        return txHandle;
    }

    /*
     * ------------------------------------ Vertex Handling ------------------------------------
     */

    @Override
    public boolean containsVertex(final long vertexid) {
        verifyOpen();
        if (vertexCache.contains(vertexid)) {
            if (!vertexCache.get(vertexid, vertexConstructor).isRemoved()) return true;
            else return false;
        } else if (vertexid > 0 && graph.containsVertexID(vertexid, txHandle)) return true;
        else return false;
    }

    @Override
    public TitanVertex getVertex(final long id) {
        verifyOpen();
        if (config.hasVerifyVertexExistence() && !containsVertex(id)) return null;
        return getExistingVertex(id);
    }

    public InternalVertex getExistingVertex(final long id) {
        //return vertex no matter what, even if deleted
        InternalVertex vertex = vertexCache.get(id, vertexConstructor);
        return vertex;
    }

    private final Retriever<Long, InternalVertex> vertexConstructor = new Retriever<Long, InternalVertex>() {
        @Override
        public InternalVertex get(Long id) {
            Preconditions.checkNotNull(id);
            Preconditions.checkArgument(id > 0);

            InternalVertex vertex = null;
            if (idInspector.isTypeID(id)) {
                Preconditions.checkArgument(id > 0);
                if (idInspector.isPropertyKeyID(id)) {
                    vertex = new TitanKeyVertex(StandardTitanTx.this, id, ElementLifeCycle.Loaded);
                } else {
                    Preconditions.checkArgument(idInspector.isEdgeLabelID(id));
                    vertex = new TitanLabelVertex(StandardTitanTx.this, id, ElementLifeCycle.Loaded);
                }
                //If its a newly created type, add to type cache
                typeCache.put(((TitanType) vertex).getName(), (TitanType) vertex);
            } else if (idInspector.isVertexID(id)) {
                vertex = new CacheVertex(StandardTitanTx.this, id, ElementLifeCycle.Loaded);
            } else throw new IllegalArgumentException("ID could not be recognized");
            return vertex;
        }
    };


    @Override
    public TitanVertex addVertex() {
        verifyWriteAccess();
        StandardVertex vertex = new StandardVertex(this, temporaryID.decrementAndGet(), ElementLifeCycle.New);
        vertex.addProperty(SystemKey.VertexState, (byte) 0);
        if (config.hasAssignIDsImmediately()) graph.assignID(vertex);
        vertexCache.add(vertex, vertex.getID());
        return vertex;
    }


    @Override
    public Iterable<Vertex> getVertices() {
        if (!addedRelations.isEmpty()) {
            //There are possible new vertices
            List<Vertex> newVs = new ArrayList<Vertex>();
            for (InternalVertex v : vertexCache.getAll()) {
                if (v.isNew() && !(v instanceof TitanType)) newVs.add(v);
            }
            return Iterables.concat(newVs, new VertexIterable(graph, this));
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
            deletedRelations.put(Long.valueOf(relation.getID()), relation);
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
                            "An edge with the given type already exists on the out-vertex");
                }
                if (label.isUnique(Direction.IN)) {
                    Preconditions.checkArgument(Iterables.isEmpty(query(inVertex).includeHidden().type(label).direction(Direction.IN).titanEdges()),
                            "An edge with the given type already exists on the in-vertex");
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
                            "An property with the given key already exists on the vertex and the property key is defined as out-unique");
                }
                if (key.isUnique(Direction.IN)) {
                    Preconditions.checkArgument(Iterables.isEmpty(getVertices(key, value)),
                            "The given value is already used as a property and the property key is defined as in-unique");
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
        addProperty(type, SystemKey.TypeName, name);
        addProperty(type, SystemKey.TypeClass, typeClass);
        for (TypeAttribute attribute : definition.getAttributes()) {
            addProperty(type, SystemKey.TypeDefinition, attribute);
        }
        graph.assignID(type);
        Preconditions.checkArgument(type.getID() > 0);
        vertexCache.add(type, type.getID());
        typeCache.put(name, type);
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
        return (typeCache.containsKey(name) || !Iterables.isEmpty(getVertices(SystemKey.TypeName, name)));
    }

    @Override
    public TitanType getType(String name) {
        verifyOpen();
        TitanType type = typeCache.get(name);
        if (type == null) type = (TitanType) Iterables.getOnlyElement(getVertices(SystemKey.TypeName, name), null);
        return type;
    }

    public TitanType getExistingType(long typeid) {
        Preconditions.checkArgument(idInspector.isTypeID(typeid), "Provided id [%s] is not a type id", typeid);
        if (SystemTypeManager.isSystemRelationType(typeid)) {
            return SystemTypeManager.getSystemRelationType(typeid);
        } else {
            InternalVertex v = getExistingVertex(typeid);
            Preconditions.checkArgument(v instanceof TitanType, "Given id is not a type: " + typeid);
            return (TitanType) v;
        }
    }

    @Override
    public TitanKey getPropertyKey(String name) {
        TitanType et = getType(name);
        if (et == null) {
            return config.getAutoEdgeTypeMaker().makeKey(name, makeType());
        } else if (et.isPropertyKey()) {
            return (TitanKey) et;
        } else
            throw new IllegalArgumentException("The type of given name is not a key: " + name);

    }

    @Override
    public TitanLabel getEdgeLabel(String name) {
        TitanType et = getType(name);
        if (et == null) {
            return config.getAutoEdgeTypeMaker().makeLabel(name, makeType());
        } else if (et.isEdgeLabel()) {
            return (TitanLabel) et;
        } else
            throw new IllegalArgumentException("The type of given name is not a label: " + name);
    }

    @Override
    public TypeMaker makeType() {
        return new StandardTypeMaker(this, graph.getIndexSerializer());
    }

    /*
     * ------------------------------------ Query Answering ------------------------------------
     */

    public VertexCentricQueryBuilder query(TitanVertex vertex) {
        return new VertexCentricQueryBuilder((InternalVertex) vertex, graph.getEdgeSerializer());
    }

    @Override
    public TitanMultiVertexQuery multiQuery(TitanVertex... vertices) {
        MultiVertexCentricQueryBuilder builder = new MultiVertexCentricQueryBuilder(this, graph.getEdgeSerializer());
        for (TitanVertex v : vertices) builder.addVertex(v);
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
            if (query.getVertex().isNew() || (query.getVertex()).hasAddedRelations()) {
                return (Iterator) (query.getVertex()).getAddedRelations(new Predicate<InternalRelation>() {
                    //Need to filter out self-loops if query only asks for one direction

                    private TitanRelation previous = null;

                    @Override
                    public boolean apply(@Nullable InternalRelation relation) {
                        if ((relation instanceof TitanEdge) && relation.isLoop()
                                && query.getDirection() != Direction.BOTH) {
                            if (relation.equals(previous)) return false;
                            else previous = relation;
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
            return deletedRelations.containsKey(Long.valueOf(result.getID())) || result != ((InternalRelation) result).it();
        }

        @Override
        public Iterator<TitanRelation> execute(final VertexCentricQuery query, final SliceQuery sq, final Object exeInfo) {
            if (query.getVertex().isNew()) return Iterators.emptyIterator();

            final boolean filterDirection = (exeInfo != null) ? (Boolean) exeInfo : false;
            final EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
            final InternalVertex v = query.getVertex();
            Iterable<TitanRelation> result = null;

            Iterable<Entry> iter = v.loadRelations(sq, new Retriever<SliceQuery, List<Entry>>() {
                @Override
                public List<Entry> get(SliceQuery query) {
                    return graph.edgeQuery(v.getID(), query, txHandle);
                }
            });

            if (filterDirection) {
                Preconditions.checkArgument(query.getDirection() != Direction.BOTH);
                iter = Iterables.filter(iter, new Predicate<Entry>() {
                    @Override
                    public boolean apply(@Nullable Entry entry) {
                        return edgeSerializer.parseDirection(entry) == query.getDirection();
                    }
                });
            }

            result = Iterables.transform(iter, new Function<Entry, TitanRelation>() {
                @Nullable
                @Override
                public TitanRelation apply(@Nullable Entry entry) {
                    return edgeSerializer.readRelation(v, entry);
                }
            });


            return result.iterator();
        }

    };


    public final QueryExecutor<GraphCentricQuery, TitanElement, IndexQuery> elementProcessor = new QueryExecutor<GraphCentricQuery, TitanElement, IndexQuery>() {

        private final PredicateCondition<TitanKey, TitanElement> getEqualityCondition(Condition<TitanElement> condition) {
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
            Preconditions.checkArgument(query.getType() == ElementType.VERTEX || query.getType() == ElementType.EDGE);
            if (query.getType() == ElementType.VERTEX && hasModifications()) {
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
            } else if (query.getType() == ElementType.EDGE && !addedRelations.isEmpty()) {
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
            else if (query.getType() == ElementType.VERTEX) {
                Preconditions.checkArgument(result instanceof InternalVertex);
                InternalVertex v = ((InternalVertex) result).it();
                if (v.hasAddedRelations() || v.hasRemovedRelations()) {
                    return !query.matches(result);
                } else return false;
            } else if (query.getType() == ElementType.EDGE) {
                //Loaded edges are immutable and new edges are previously filtered
                Preconditions.checkArgument(result.isLoaded() || result.isNew());
                return false;
            } else throw new IllegalArgumentException("Unexpected type: " + query.getType());
        }

        @Override
        public Iterator<TitanElement> execute(final GraphCentricQuery query, final IndexQuery indexQuery, final Object exeInfo) {
            Iterator<TitanElement> iter = null;
            if (!indexQuery.hasStore()) {
                log.warn("Query requires iterating over all vertices [{}]. For better performance, use indexes", query.getCondition());
                if (query.getType() == ElementType.VERTEX) {
                    iter = (Iterator) getVertices().iterator();
                } else if (query.getType() == ElementType.EDGE) {
                    iter = (Iterator) getEdges().iterator();
                } else throw new IllegalArgumentException("Unexpected type: " + query.getType());
            } else {
                Preconditions.checkArgument(exeInfo != null && exeInfo instanceof String);
                final String indexName = (String) exeInfo;
                try {
                    iter = Iterators.transform(indexCache.get(indexQuery, new Callable<List<Object>>() {
                        @Override
                        public List<Object> call() throws Exception {
                            return graph.elementQuery(indexName, indexQuery, txHandle);
                        }
                    }).iterator(), new Function<Object, TitanElement>() {
                        @Nullable
                        @Override
                        public TitanElement apply(@Nullable Object id) {
                            Preconditions.checkNotNull(id);
                            if (id instanceof Long) return getExistingVertex((Long) id);
                            else if (id instanceof RelationIdentifier)
                                return (TitanElement) getEdge((RelationIdentifier) id);
                            else throw new IllegalArgumentException("Unexpected id type: " + id);
                        }
                    });
                } catch (Exception e) {
                    throw new TitanException("Could not call index", e);
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
        }
    }

    private void close() {
        //TODO: release non crucial data structures to preserve memory?
        isOpen = false;
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
