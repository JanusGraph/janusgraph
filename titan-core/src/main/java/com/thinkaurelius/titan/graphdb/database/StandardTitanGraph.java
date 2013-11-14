package com.thinkaurelius.titan.graphdb.database;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.thinkaurelius.titan.graphdb.blueprints.TitanFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.cache.StoreCache;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.thinkaurelius.titan.graphdb.util.ExceptionFactory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Features;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

public class StandardTitanGraph extends TitanBlueprintsGraph {

    private static final Logger log =
            LoggerFactory.getLogger(StandardTitanGraph.class);

    private final GraphDatabaseConfiguration config;
    private final IDManager idManager;
    private final VertexIDAssigner idAssigner;
    private boolean isOpen;

    private final Backend backend;

    private final int maxWriteRetryAttempts;
    private final int retryStorageWaitTime;

    protected final IndexSerializer indexSerializer;
    protected final EdgeSerializer edgeSerializer;
    protected final Serializer serializer;

    public final SliceQuery vertexExistenceQuery;

    private final RelationQueryCache relationCache;
    private final StoreCache edgeStoreCache;

    public StandardTitanGraph(GraphDatabaseConfiguration configuration) {
        this.config = configuration;
        this.backend = configuration.getBackend();
        this.maxWriteRetryAttempts = config.getWriteAttempts();
        this.retryStorageWaitTime = config.getStorageWaittime();

        this.idAssigner = config.getIDAssigner(backend);
        this.idManager = idAssigner.getIDManager();

        this.serializer = config.getSerializer();
        this.indexSerializer = new IndexSerializer(this.serializer, this.backend.getIndexInformation());
        this.edgeSerializer = new EdgeSerializer(this.serializer);
        this.vertexExistenceQuery = edgeSerializer.getQuery(SystemKey.VertexState, Direction.OUT, new EdgeSerializer.TypedInterval[0], null).setLimit(1);
        this.relationCache = new RelationQueryCache(this.edgeSerializer);
        this.edgeStoreCache = config.getEdgeStoreCache();
        isOpen = true;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }
    
    public void clear() throws StorageException {
        backend.clearStorage();
    }

    @Override
    public synchronized void shutdown() throws TitanException {
        if (!isOpen) return;
        try {
            super.shutdown();
            idAssigner.close();
            backend.close();
            edgeStoreCache.close();
            relationCache.close();
        } catch (StorageException e) {
            throw new TitanException("Could not close storage backend", e);
        } finally {
            isOpen = false;
        }
    }


    @Override
    public Features getFeatures() {
        return TitanFeatures.getFeatures(getConfiguration(), backend.getStoreFeatures());
    }

    @Override
    public TitanTransaction newTransaction() {
        return buildTransaction().start();
    }

    @Override
    public StandardTransactionBuilder buildTransaction() {
        return new StandardTransactionBuilder(getConfiguration(), this);
    }

    @Override
    public TitanTransaction newThreadBoundTransaction() {
        return buildTransaction().threadBound().start();
    }

    public StandardTitanTx newTransaction(TransactionConfiguration configuration) {
        if (!isOpen) ExceptionFactory.graphShutdown();
        try {
            IndexSerializer.IndexInfoRetriever retriever = indexSerializer.getIndexInforRetriever();
            StandardTitanTx tx = new StandardTitanTx(this, configuration, backend.beginTransaction(configuration,retriever));
            retriever.setTransaction(tx);
            return tx;
        } catch (StorageException e) {
            throw new TitanException("Could not start new transaction", e);
        }
    }

    public IndexSerializer getIndexSerializer() {
        return indexSerializer;
    }

    public IDInspector getIDInspector() {
        return idManager.getIDInspector();
    }

    public EdgeSerializer getEdgeSerializer() {
        return edgeSerializer;
    }

    public AttributeHandling getAttributeHandling() {
        return serializer;
    }

    public RelationQueryCache getRelationCache() {
        return relationCache;
    }

    public GraphDatabaseConfiguration getConfiguration() {
        return config;
    }

    // ################### READ #########################

    public RecordIterator<Long> getVertexIDs(final BackendTransaction tx) {
        Preconditions.checkArgument(backend.getStoreFeatures().supportsOrderedScan() ||
                backend.getStoreFeatures().supportsUnorderedScan(),
                "The configured storage backend does not support global graph operations - use Faunus instead");

        final KeyIterator keyiter;
        if (backend.getStoreFeatures().supportsUnorderedScan()) {
            keyiter = tx.edgeStoreKeys(vertexExistenceQuery);
        } else {
            keyiter = tx.edgeStoreKeys(new KeyRangeQuery(IDHandler.MIN_KEY, IDHandler.MAX_KEY, vertexExistenceQuery));
        }

        return new RecordIterator<Long>() {

            @Override
            public boolean hasNext() {
                return keyiter.hasNext();
            }

            @Override
            public Long next() {
                return IDHandler.getKeyID(keyiter.next());
            }

            @Override
            public void close() throws IOException {
                keyiter.close();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Removal not supported");
            }
        };
    }

    public List<Entry> edgeQuery(long vid, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(vid > 0);
        return edgeStoreCache.query(new KeySliceQuery(IDHandler.getKey(vid), query),tx);
    }

    public List<List<Entry>> edgeMultiQuery(LongArrayList vids, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(vids != null && !vids.isEmpty());
        List<StaticBuffer> vertexIds = new ArrayList<StaticBuffer>(vids.size());
        for (int i = 0; i < vids.size(); i++) {
            Preconditions.checkArgument(vids.get(i) > 0);
            vertexIds.add(IDHandler.getKey(vids.get(i)));
        }
        return edgeStoreCache.multiQuery(vertexIds, query, tx);
    }


    // ################### WRITE #########################

    public void assignID(InternalElement vertex) {
        idAssigner.assignID(vertex);
    }

    public void commit(final Collection<InternalRelation> addedRelations,
                     final Collection<InternalRelation> deletedRelations, final StandardTitanTx tx) {
        //Setup
        log.debug("Saving transaction. Added {}, removed {}", addedRelations.size(), deletedRelations.size());

        final BackendTransaction mutator = tx.getTxHandle();
        final boolean acquireLocks = tx.getConfiguration().hasAcquireLocks();

        //1. Assign TitanVertex IDs
        if (!tx.getConfiguration().hasAssignIDsImmediately())
            idAssigner.assignIDs(addedRelations);

        Callable<List<StaticBuffer>> persist = new Callable<List<StaticBuffer>>() {
            @Override
            public List<StaticBuffer> call() throws Exception {
                //2. Collect deleted edges
                ListMultimap<InternalVertex, InternalRelation> mutations = ArrayListMultimap.create();
                if (deletedRelations != null && !deletedRelations.isEmpty()) {
                    for (InternalRelation del : deletedRelations) {
                        Preconditions.checkArgument(del.isRemoved());
                        for (int pos = 0; pos < del.getLen(); pos++) {
                            InternalVertex vertex = del.getVertex(pos);
                            if (pos == 0 || !del.isLoop()) mutations.put(vertex, del);
                            Direction dir = EdgeDirection.fromPosition(pos);
                            if (acquireLocks && del.getType().isUnique(dir) &&
                                    ((InternalType) del.getType()).uniqueLock(dir)) {
                                Entry entry = edgeSerializer.writeRelation(del, pos, tx);
                                mutator.acquireEdgeLock(IDHandler.getKey(vertex.getID()), entry.getColumn(), entry.getValue());
                            }
                        }
                        //Update Indexes
                        if (del.isProperty()) {
                            if (acquireLocks) indexSerializer.lockKeyedProperty((TitanProperty) del, mutator);
                        }

                    }
                }

                ListMultimap<InternalType, InternalRelation> otherEdgeTypes = ArrayListMultimap.create();

                //3. Sort Added Edges
                for (InternalRelation relation : addedRelations) {
                    Preconditions.checkArgument(relation.isNew());

                    TitanType type = relation.getType();

                    //Give special treatment to edge type definitions
                    if (SystemTypeManager.prepersistedSystemTypes.contains(type)) {
                        InternalType itype = (InternalType) relation.getVertex(0);
                        otherEdgeTypes.put(itype, relation);
                    } else { //STANDARD TitanRelation
                        for (int pos = 0; pos < relation.getLen(); pos++) {
                            InternalVertex vertex = relation.getVertex(pos);
                            if (pos == 0 || !relation.isLoop()) mutations.put(vertex, relation);
                            Direction dir = EdgeDirection.fromPosition(pos);
                            if (acquireLocks && relation.getType().isUnique(dir) && !vertex.isNew()
                                    && ((InternalType) relation.getType()).uniqueLock(dir)) {
                                Entry entry = edgeSerializer.writeRelation(relation, pos, false, tx);
                                mutator.acquireEdgeLock(IDHandler.getKey(vertex.getID()), entry.getColumn(), null);
                            }
                        }
                    }
                    //Update Indexes
                    if (relation.isProperty()) {
                        if (acquireLocks) indexSerializer.lockKeyedProperty((TitanProperty) relation, mutator);
                    }

                }

                //3. Persist
                List<StaticBuffer> mutatedVertexKeys = new ArrayList<StaticBuffer>();
                if (!otherEdgeTypes.isEmpty()) {
                    mutatedVertexKeys.addAll(persist(otherEdgeTypes, tx));
                    mutator.flush();
                    //Register new keys with indexprovider
                    for (InternalType itype : otherEdgeTypes.keySet()) {
                        if (itype.isPropertyKey() && itype.isNew())
                            indexSerializer.newPropertyKey((TitanKey) itype, mutator);
                    }
                }

                if (!mutations.isEmpty()) mutatedVertexKeys.addAll(persist(mutations, tx));
                mutator.commit();
                return mutatedVertexKeys;
            }

            @Override
            public String toString() {
                return "PersistingTransaction";
            }
        };
        List<StaticBuffer> mutatedVertexKeys = BackendOperation.execute(persist, maxWriteRetryAttempts, retryStorageWaitTime);
        for (StaticBuffer vertexKey : mutatedVertexKeys) edgeStoreCache.invalidate(vertexKey);
    }


    private <V extends InternalVertex> List<StaticBuffer> persist(ListMultimap<V, InternalRelation> mutatedEdges,
                                                    StandardTitanTx tx) throws StorageException {
        assert mutatedEdges != null && !mutatedEdges.isEmpty();

        Collection<V> vertices = mutatedEdges.keySet();

        BackendTransaction mutator = tx.getTxHandle();
        List<StaticBuffer> mutatedKeys = new ArrayList<StaticBuffer>(vertices.size());
        for (V vertex : vertices) {
            Preconditions.checkArgument(vertex.getID() > 0, "Vertex has no id: %s", vertex.getID());
            List<InternalRelation> edges = mutatedEdges.get(vertex);
            List<Entry> additions = new ArrayList<Entry>(edges.size());
            List<StaticBuffer> deletions = new ArrayList<StaticBuffer>(Math.max(10, edges.size() / 10));
            for (InternalRelation edge : edges) {
                for (int pos = 0; pos < edge.getLen(); pos++) {
                    if (edge.getVertex(pos).equals(vertex)) {
                        if (edge.isRemoved()) {
                            deletions.add(edgeSerializer.writeRelation(edge, pos, false, tx).getColumn());
                        } else {
                            Preconditions.checkArgument(edge.isNew());
                            additions.add(edgeSerializer.writeRelation(edge, pos, tx));
                        }
                    }
                }
            }

            StaticBuffer vertexKey = IDHandler.getKey(vertex.getID());
            mutator.mutateEdges(vertexKey, additions, deletions);
            if (!vertex.isNew()) mutatedKeys.add(vertexKey);
            //Index Updates
            for (InternalRelation relation : edges) {
                if (relation.getVertex(0).equals(vertex)) {
                    if (relation.isRemoved()) {
                        if (relation.isProperty()) {
                            indexSerializer.removeProperty((TitanProperty) relation, mutator);
                        } else if (relation.isEdge()) {
                            indexSerializer.removeEdge(relation, mutator);
                        }
                    } else {
                        Preconditions.checkArgument(relation.isNew());
                        if (relation.isProperty()) {
                            indexSerializer.addProperty((TitanProperty) relation, mutator);
                        } else {
                            indexSerializer.addEdge(relation, mutator);
                        }
                    }
                }
            }
        }
        return mutatedKeys;
    }

}
