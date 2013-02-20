package com.thinkaurelius.titan.graphdb.transaction;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.graphdb.adjacencylist.StandardAdjListFactory;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsTransaction;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.ElementQuery;
import com.thinkaurelius.titan.graphdb.query.VertexQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.AddedRelationsContainer;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.ConcurrentBufferAddedRelations;
import com.thinkaurelius.titan.graphdb.transaction.addedrelations.SimpleBufferAddedRelations;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.ConcurrentVertexCache;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.SimpleVertexCache;
import com.thinkaurelius.titan.graphdb.transaction.vertexcache.VertexCache;
import com.thinkaurelius.titan.graphdb.types.EdgeLabelDefinition;
import com.thinkaurelius.titan.graphdb.types.PropertyKeyDefinition;
import com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanLabelVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class StandardTitanTx extends TitanBlueprintsTransaction {

    private static final Logger log = LoggerFactory.getLogger(AbstractTitanTx.class);

    private static final Map<Long,InternalRelation> EMPTY_DELETED_RELATIONS = ImmutableMap.of();
    private static final ConcurrentMap<UniqueLockApplication,Lock> UNINITIALIZED_LOCKS = null;

    private static final long DEFAULT_CACHE_SIZE = 10000;

    private final StandardTitanGraph graphdb;
    private final TransactionConfig config;
    private final IDInspector idInspector;
    private final TransactionHandle txHandle;

    //Internal data structures
    private final VertexCache vertexCache;
    private final AtomicLong temporaryVertexID;
    private final AddedRelationsContainer addedRelations;
    private Map<Long,InternalRelation> deletedRelations;
    private final Cache<ElementQuery,List<Object>> indexCache;
    private final Map<String,InternalType> typeCache;
    private ConcurrentMap<UniqueLockApplication,Lock> uniqueLocks;

    private boolean isOpen;


    public StandardTitanTx(StandardTitanGraph graphdb, TransactionConfig config, IDInspector idInspector, TransactionHandle txHandle) {
        Preconditions.checkNotNull(graphdb);
        Preconditions.checkArgument(graphdb.isOpen());
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(idInspector);
        Preconditions.checkNotNull(txHandle);
        this.graphdb = graphdb;
        this.config = config;
        this.idInspector = idInspector;
        this.txHandle = txHandle;

        temporaryVertexID = new AtomicLong(-1);
        CacheBuilder<ElementQuery,List<Object>> indexCacheBuilder = CacheBuilder.newBuilder().
                weigher(new Weigher<ElementQuery, List<Object>>() {
                    @Override
                    public int weigh(ElementQuery q, List<Object> r) {
                        return 2 + r.size();
                    }
                }).
                maximumWeight(DEFAULT_CACHE_SIZE);
        if (config.isSingleThreaded()) {
            vertexCache = new SimpleVertexCache();
            addedRelations = new SimpleBufferAddedRelations();
            indexCache = indexCacheBuilder.concurrencyLevel(1).build();
            typeCache = new HashMap<String,InternalType>();
        } else {
            vertexCache = new ConcurrentVertexCache();
            addedRelations = new ConcurrentBufferAddedRelations();
            indexCache = indexCacheBuilder.build();
            typeCache = new ConcurrentHashMap<String, InternalType>();

        }
        uniqueLocks = UNINITIALIZED_LOCKS;
        deletedRelations = EMPTY_DELETED_RELATIONS;
        this.isOpen = true;
    }

    public void removeRelation(InternalRelation relation) {
        //Update transaction data structures
        if (relation.isNew()) {
            addedRelations.remove(relation);
            //Delete from Vertex
            for (int i=0;i<relation.getArity();i++) {
                relation.getVertex(i).removeRelation(relation);
            }
        } else {
            if (deletedRelations == EMPTY_DELETED_RELATIONS) {
                if (config.isSingleThreaded()) {
                    deletedRelations = new HashMap<Long,InternalRelation>();
                } else {
                    synchronized (this) {
                        if (deletedRelations == EMPTY_DELETED_RELATIONS)
                            deletedRelations = new ConcurrentHashMap<Long, InternalRelation>();
                    }
                }
            }
            deletedRelations.put(Long.valueOf(relation.getID()),relation);
        }
    }

    private Lock getUniquenessLock(final TitanVertex start, final TitanType type, final Object end) {
        if (uniqueLocks==UNINITIALIZED_LOCKS) {
            Preconditions.checkArgument(!config.isSingleThreaded());
            synchronized (this) {
                if (uniqueLocks==UNINITIALIZED_LOCKS)
                    uniqueLocks = new ConcurrentHashMap<UniqueLockApplication, Lock>();
            }
        }
        UniqueLockApplication la = new UniqueLockApplication(start,type,end);
        return uniqueLocks.putIfAbsent(la,new ReentrantLock());
    }

    public StandardTitanTx getNextTx() {
        Preconditions.checkArgument(isClosed());
        if (!config.isThreadBound()) throw new IllegalStateException("Cannot access vertex since its enclosing transaction is closed and unbound");
        else return (StandardTitanTx)graphdb.getCurrentThreadTx();
    }

    private final InternalVertex getInternal(Vertex vertex) {
        assert vertex instanceof InternalVertex;
        Preconditions.checkArgument(((InternalVertex)vertex).tx()==this,"Given vertex does not belong to this transaction");
        return ((InternalVertex)vertex).it();
    }

    public TransactionConfig getConfiguration() {
        return config;
    }

    public VertexQueryBuilder query(TitanVertex vertex) {

    }

    private final void verifyTxState() {
        Preconditions.checkArgument(isOpen(),"Transaction has been closed");
    }

    public InternalVertex getExistingVertex(long id) {
        //return vertex no matter what, even if deleted

        //If its a newly created type, add to type cache
    }

    public TitanKey makePropertyKey(PropertyKeyDefinition definition) {
        verifyTxState();
        TitanKeyVertex prop = new TitanKeyVertex(this, temporaryVertexID.decrementAndGet(), ElementLifeCycle.New);
        addProperty(prop, SystemKey.PropertyTypeDefinition, definition);
        addProperty(prop, SystemKey.TypeName, definition.getName());
        addProperty(prop, SystemKey.TypeClass, TitanTypeClass.KEY);
        //TODO: add to internal structures etc
        return prop;
    }

    public TitanLabel makeEdgeLabel(EdgeLabelDefinition definition) {
        verifyTxState();
        TitanLabelVertex prop = new TitanLabelVertex(this, temporaryVertexID.decrementAndGet(), ElementLifeCycle.New);
        addProperty(prop, SystemKey.EdgeTypeDefinition, definition);
        addProperty(prop, SystemKey.TypeName, definition.getName());
        addProperty(prop, SystemKey.TypeClass, TitanTypeClass.LABEL);
        //TODO: add to internal structures etc
        return prop;
    }

    @Override
    public TitanVertex addVertex() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, TitanLabel label) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanEdge addEdge(TitanVertex outVertex, TitanVertex inVertex, String label) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanProperty addProperty(TitanVertex vertex, TitanKey key, Object attribute) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanProperty addProperty(TitanVertex vertex, String key, Object attribute) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setProperty(TitanVertex vertex, final TitanKey key, Object value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkArgument(key.isUnique(Direction.OUT));

        if (config.hasVerifyUniqueness()) {
            //Acquire uniqueness lock, remove and add
            removeProperty(key);
        } else {
            //Only delete in-memory
            for (InternalRelation r : it().getAddedRelations(new Predicate<InternalRelation>() {
                @Override
                public boolean apply(@Nullable InternalRelation internalRelation) {
                    return internalRelation.getType().equals(key);
                }
            })) {
                r.remove();
            }
        }
        if (pkey.isUnique(Direction.OUT)) {
            TitanProperty existing = Iterables.getOnlyElement(vertex.getProperties(pkey), null);
            if (existing != null) existing.remove();
        }
        vertex.addProperty(pkey, value);


        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanVertex getVertex(long id) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsVertex(long vertexid) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanQuery query(TitanVertex vertex) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<TitanVertex> getVertices(TitanKey key, Object attribute) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object attribute) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<Edge> getEdges() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsType(String name) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TitanType getType(String name) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void commit() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void rollback() {
        //To change body of implemented methods use File | Settings | File Templates.
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
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
