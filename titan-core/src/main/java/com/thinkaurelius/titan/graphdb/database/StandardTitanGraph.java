package com.thinkaurelius.titan.graphdb.database;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.core.UserModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.thinkaurelius.titan.graphdb.blueprints.TitanFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.cache.SchemaCache;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.serialize.AttributeHandling;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration;
import com.thinkaurelius.titan.graphdb.types.ExternalIndexType;
import com.thinkaurelius.titan.graphdb.types.InternalIndexType;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemRelationType;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanTypeVertex;
import com.thinkaurelius.titan.graphdb.util.ExceptionFactory;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Features;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class StandardTitanGraph extends TitanBlueprintsGraph {

    private static final Logger log =
            LoggerFactory.getLogger(StandardTitanGraph.class);


    private final GraphDatabaseConfiguration config;
    private final Backend backend;
    private final IDManager idManager;
    private final VertexIDAssigner idAssigner;

    private final int maxWriteRetryAttempts;
    private final int retryStorageWaitTime;

    //Serializers
    protected final IndexSerializer indexSerializer;
    protected final EdgeSerializer edgeSerializer;
    protected final Serializer serializer;

    //Caches
    public final SliceQuery vertexExistenceQuery;
    private final RelationQueryCache queryCache;
    private final SchemaCache schemaCache;

    private boolean isOpen;


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
        this.vertexExistenceQuery = edgeSerializer.getQuery(SystemKey.VertexExists, Direction.OUT, new EdgeSerializer.TypedInterval[0], null).setLimit(1);
        this.queryCache = new RelationQueryCache(this.edgeSerializer);
        this.schemaCache = configuration.getTypeCache(typeCacheRetrieval);
        isOpen = true;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public synchronized void shutdown() throws TitanException {
        if (!isOpen) return;
        try {
            super.shutdown();
            idAssigner.close();
            backend.close();
            queryCache.close();
        } catch (StorageException e) {
            throw new TitanException("Could not close storage backend", e);
        } finally {
            isOpen = false;
        }
    }

    // ################### Simple Getters #########################

    @Override
    public Features getFeatures() {
        return TitanFeatures.getFeatures(getConfiguration(), backend.getStoreFeatures());
    }

    public IndexSerializer getIndexSerializer() {
        return indexSerializer;
    }

    public IDInspector getIDInspector() {
        return idManager.getIdInspector();
    }

    public EdgeSerializer getEdgeSerializer() {
        return edgeSerializer;
    }

    public AttributeHandling getAttributeHandling() {
        return serializer;
    }

    public RelationQueryCache getQueryCache() {
        return queryCache;
    }

    public SchemaCache getSchemaCache() {
        return schemaCache;
    }

    public GraphDatabaseConfiguration getConfiguration() {
        return config;
    }

    @Override
    public UserModifiableConfiguration getGlobalConfiguration() {
        return GraphDatabaseConfiguration.getGlobalUserConfig(backend.getStoreManager());
    }

    // ################### TRANSACTIONS #########################

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
            IndexSerializer.IndexInfoRetriever retriever = indexSerializer.getIndexInfoRetriever();
            StandardTitanTx tx = new StandardTitanTx(this, configuration, backend.beginTransaction(configuration,retriever));
            retriever.setTransaction(tx);
            return tx;
        } catch (StorageException e) {
            throw new TitanException("Could not start new transaction", e);
        }
    }

    // ################### READ #########################

    private final SchemaCache.StoreRetrieval typeCacheRetrieval = new SchemaCache.StoreRetrieval() {

        @Override
        public Long retrieveTypeByName(String typeName, StandardTitanTx tx) {
            TitanVertex v = Iterables.getOnlyElement(tx.getVertices(SystemKey.TypeName, typeName),null);
            return v!=null?v.getID():null;
        }

        @Override
        public EntryList retrieveTypeRelations(final long schemaId, final SystemRelationType type, final Direction dir, final StandardTitanTx tx) {
            SliceQuery query = queryCache.getQuery(type,dir);
            return edgeQuery(schemaId, query, tx.getTxHandle());
        }

    };

    public RecordIterator<Long> getVertexIDs(final BackendTransaction tx) {
        Preconditions.checkArgument(backend.getStoreFeatures().hasOrderedScan() ||
                backend.getStoreFeatures().hasUnorderedScan(),
                "The configured storage backend does not support global graph operations - use Faunus instead");

        final KeyIterator keyiter;
        if (backend.getStoreFeatures().hasUnorderedScan()) {
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

    public EntryList edgeQuery(long vid, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(vid > 0);
        return tx.edgeStoreQuery(new KeySliceQuery(IDHandler.getKey(vid), query));
    }

    public List<EntryList> edgeMultiQuery(LongArrayList vids, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(vids != null && !vids.isEmpty());
        List<StaticBuffer> vertexIds = new ArrayList<StaticBuffer>(vids.size());
        for (int i = 0; i < vids.size(); i++) {
            Preconditions.checkArgument(vids.get(i) > 0);
            vertexIds.add(IDHandler.getKey(vids.get(i)));
        }
        Map<StaticBuffer,EntryList> result = tx.edgeStoreMultiQuery(vertexIds, query);
        List<EntryList> resultList = new ArrayList<EntryList>(result.size());
        for (StaticBuffer v : vertexIds) resultList.add(result.get(v));
        return resultList;
    }


    // ################### WRITE #########################

    public void assignID(InternalElement vertex) {
        idAssigner.assignID(vertex);
    }

    public static boolean acquireLock(InternalRelation relation, int pos, boolean acquireLocksConfig) {
        InternalRelationType type = (InternalRelationType)relation.getType();
        return acquireLocksConfig && type.getConsistencyModifier()==ConsistencyModifier.LOCK &&
                ( type.getMultiplicity().isUnique(EdgeDirection.fromPosition(pos))
                        || pos==0 && type.getMultiplicity()==Multiplicity.SIMPLE);
    }

    public static boolean acquireLock(InternalIndexType index, boolean acquireLocksConfig) {
        return acquireLocksConfig && index.getConsistencyModifier()==ConsistencyModifier.LOCK
                && index.getCardinality()!=Cardinality.LIST;
    }

    public Callable<Boolean> persist(final Collection<InternalRelation> addedRelations,
                                     final Collection<InternalRelation> deletedRelations,
                                     final Predicate<InternalRelation> filter,
                                     final StandardTitanTx tx, final boolean commit) {
        final BackendTransaction mutator = tx.getTxHandle();
        final boolean acquireLocks = tx.getConfiguration().hasAcquireLocks();

        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ListMultimap<InternalVertex, InternalRelation> mutations = ArrayListMultimap.create();
                List<IndexSerializer.IndexUpdate> indexUpdates = Lists.newArrayList();
                //1) Collect deleted edges and their index updates and acquire edge locks
                for (InternalRelation del : Iterables.filter(deletedRelations,filter)) {
                    Preconditions.checkArgument(del.isRemoved());
                    for (int pos = 0; pos < del.getLen(); pos++) {
                        InternalVertex vertex = del.getVertex(pos);
                        if (pos == 0 || !del.isLoop()) mutations.put(vertex, del);
                        if (acquireLock(del,pos,acquireLocks)) {
                            Entry entry = edgeSerializer.writeRelation(del, pos, tx);
                            mutator.acquireEdgeLock(IDHandler.getKey(vertex.getID()), entry);
                        }
                    }
                    indexUpdates.addAll(indexSerializer.getIndexUpdates(del));
                }

                //2) Collect added edges and their index updates and acquire edge locks
                for (InternalRelation add : Iterables.filter(addedRelations,filter)) {
                    Preconditions.checkArgument(add.isNew());

                    for (int pos = 0; pos < add.getLen(); pos++) {
                        InternalVertex vertex = add.getVertex(pos);
                        if (pos == 0 || !add.isLoop()) mutations.put(vertex, add);
                        if (!vertex.isNew() && acquireLock(add,pos,acquireLocks)) {
                            Entry entry = edgeSerializer.writeRelation(add, pos, tx);
                            mutator.acquireEdgeLock(IDHandler.getKey(vertex.getID()), entry.getColumn());
                        }
                    }
                    indexUpdates.addAll(indexSerializer.getIndexUpdates(add));
                }

                //3) Collect all index update for vertices
                for (InternalVertex v : mutations.keySet()) {
                    indexUpdates.addAll(indexSerializer.getIndexUpdates(v,mutations.get(v)));
                }
                //4) Acquire index locks (deletions first)
                for (IndexSerializer.IndexUpdate update : indexUpdates) {
                    if (!update.isInternalIndex() || !update.isDeletion()) continue;
                    InternalIndexType iIndex = (InternalIndexType) update.getIndex();
                    if (acquireLock(iIndex,acquireLocks)) {
                        mutator.acquireIndexLock((StaticBuffer)update.getKey(), (Entry)update.getEntry());
                    }
                }
                for (IndexSerializer.IndexUpdate update : indexUpdates) {
                    if (!update.isInternalIndex() || !update.isAddition()) continue;
                    InternalIndexType iIndex = (InternalIndexType) update.getIndex();
                    if (acquireLock(iIndex,acquireLocks)) {
                        mutator.acquireIndexLock((StaticBuffer)update.getKey(), ((Entry)update.getEntry()).getColumn());
                    }
                }

                //5) Add relation mutations
                BackendTransaction mutator = tx.getTxHandle();
                for (InternalVertex vertex : mutations.keySet()) {
                    Preconditions.checkArgument(vertex.getID() > 0, "Vertex has no id: %s", vertex.getID());
                    List<InternalRelation> edges = mutations.get(vertex);
                    List<Entry> additions = new ArrayList<Entry>(edges.size());
                    List<StaticBuffer> deletions = new ArrayList<StaticBuffer>(Math.max(10, edges.size() / 10));
                    for (InternalRelation edge : edges) {
                        for (int pos = 0; pos < edge.getLen(); pos++) {
                            if (edge.getVertex(pos).equals(vertex)) {
                                if (edge.isRemoved()) {
                                    deletions.add(edgeSerializer.writeRelation(edge, pos, tx).getColumn());
                                } else {
                                    Preconditions.checkArgument(edge.isNew());
                                    additions.add(edgeSerializer.writeRelation(edge, pos, tx));
                                }
                            }
                        }
                    }

                    StaticBuffer vertexKey = IDHandler.getKey(vertex.getID());
                    mutator.mutateEdges(vertexKey, additions, deletions);
                }

                //6) Add index updates
                for (IndexSerializer.IndexUpdate indexUpdate : indexUpdates) {
                    assert indexUpdate.isAddition() || indexUpdate.isDeletion();
                    if (indexUpdate.isInternalIndex()) {
                        IndexSerializer.IndexUpdate<StaticBuffer,Entry> update = indexUpdate;
                        if (update.isAddition())
                            mutator.mutateIndex(update.getKey(), ImmutableList.of(update.getEntry()),KeyColumnValueStore.NO_DELETIONS);
                        else
                            mutator.mutateIndex(update.getKey(), KeyColumnValueStore.NO_ADDITIONS, ImmutableList.of(update.getEntry().getColumn()));
                    } else {
                        IndexSerializer.IndexUpdate<String,IndexEntry> update = indexUpdate;
                        IndexTransaction itx = mutator.getIndexTransactionHandle(((ExternalIndexType)update.getIndex()).getIndexName());
                        if (update.isAddition())
                            itx.add(update.getIndex().getElement().getName(),update.getKey(),update.getEntry().field,update.getEntry().value,update.getElement().isNew());
                        else
                            itx.delete(update.getIndex().getElement().getName(),update.getKey(),update.getEntry().field,update.getEntry().value,update.getElement().isRemoved());
                    }
                }

                //TODO: Register new keys with indexprovider
//                for (InternalRelationType itype : otherEdgeTypes.keySet()) {
//                    if (itype.isPropertyKey() && itype.isNew())
//                        indexSerializer.newPropertyKey((TitanKey) itype, mutator);
//                }


                //7) Persist
                if (!mutations.isEmpty()) {
                    persist(mutations, tx);
                    //TODO: Send log message
                    if (!commit) mutator.flush();
                }
                if (commit) mutator.commit();
                return Boolean.TRUE;
            }

            @Override
            public String toString() {
                return "PersistingTransaction";
            }
        };
    }

    private static final Predicate<InternalRelation> SYSTEMTYPES_FILTER = new Predicate<InternalRelation>() {
        @Override
        public boolean apply(@Nullable InternalRelation internalRelation) {
            return internalRelation instanceof SystemRelationType && internalRelation.getVertex(0) instanceof TitanTypeVertex;
        }
    };

    private static final Predicate<InternalRelation> NO_SYSTEMTYPES_FILTER = new Predicate<InternalRelation>() {
        @Override
        public boolean apply(@Nullable InternalRelation internalRelation) {
            return !SYSTEMTYPES_FILTER.apply(internalRelation);
        }
    };

    public void commit(final Collection<InternalRelation> addedRelations,
                     final Collection<InternalRelation> deletedRelations, final StandardTitanTx tx) {
        //Setup
        log.debug("Saving transaction. Added {}, removed {}", addedRelations.size(), deletedRelations.size());


        //1. Assign TitanVertex IDs
        if (!tx.getConfiguration().hasAssignIDsImmediately())
            idAssigner.assignIDs(addedRelations);

        if (tx.getConfiguration().hasEnabledBatchLoading()) {
            BackendOperation.execute(persist(addedRelations,deletedRelations, IterablesUtil.NO_FILTER, tx,true),
                    maxWriteRetryAttempts, retryStorageWaitTime);
        } else {
            BackendOperation.execute(persist(addedRelations,deletedRelations, SYSTEMTYPES_FILTER, tx,false),
                    maxWriteRetryAttempts, retryStorageWaitTime);
            BackendOperation.execute(persist(addedRelations,deletedRelations, NO_SYSTEMTYPES_FILTER, tx,true),
                    maxWriteRetryAttempts, retryStorageWaitTime);
        }


//        Callable<Boolean> persist = new Callable<Boolean>() {
//            @Override
//            public Boolean call() throws Exception {
//                //2. Collect deleted edges
//                ListMultimap<InternalVertex, InternalRelation> mutations = ArrayListMultimap.create();
//                if (deletedRelations != null && !deletedRelations.isEmpty()) {
//                    for (InternalRelation del : deletedRelations) {
//                        Preconditions.checkArgument(del.isRemoved());
//                        for (int pos = 0; pos < del.getLen(); pos++) {
//                            InternalVertex vertex = del.getVertex(pos);
//                            if (pos == 0 || !del.isLoop()) mutations.put(vertex, del);
//                            Direction dir = EdgeDirection.fromPosition(pos);
//                            if (acquireLocks && del.getType().isUnique(dir) &&
//                                    ((InternalRelationType) del.getType()).uniqueLock(dir)) {
//                                Entry entry = edgeSerializer.writeRelation(del, pos, tx);
//                                mutator.acquireEdgeLock(IDHandler.getKey(vertex.getID()), entry);
//                            }
//                        }
//                        //Update Indexes
//                        if (del.isProperty()) {
//                            if (acquireLocks) indexSerializer.lockKeyedProperty((TitanProperty) del, mutator);
//                        }
//
//                    }
//                }
//
//                ListMultimap<InternalRelationType, InternalRelation> otherEdgeTypes = ArrayListMultimap.create();
//
//                //3. Sort Added Edges
//                for (InternalRelation relation : addedRelations) {
//                    Preconditions.checkArgument(relation.isNew());
//
//                    TitanType type = relation.getType();
//
//                    //Give special treatment to edge type definitions
//                    if (SystemTypeManager.prepersistedSystemTypes.contains(type)) {
//                        InternalRelationType itype = (InternalRelationType) relation.getVertex(0);
//                        otherEdgeTypes.put(itype, relation);
//                    } else { //STANDARD TitanRelation
//                        for (int pos = 0; pos < relation.getLen(); pos++) {
//                            InternalVertex vertex = relation.getVertex(pos);
//                            if (pos == 0 || !relation.isLoop()) mutations.put(vertex, relation);
//                            Direction dir = EdgeDirection.fromPosition(pos);
//                            if (acquireLocks && relation.getType().isUnique(dir) && !vertex.isNew()
//                                    && ((InternalRelationType) relation.getType()).uniqueLock(dir)) {
//                                Entry entry = edgeSerializer.writeRelation(relation, pos, tx);
//                                mutator.acquireEdgeLock(IDHandler.getKey(vertex.getID()), entry.getColumn(), null);
//                            }
//                        }
//                    }
//                    //Update Indexes
//                    if (relation.isProperty()) {
//                        if (acquireLocks) indexSerializer.lockKeyedProperty((TitanProperty) relation, mutator);
//                    }
//
//                }
//
//                //3. Persist
//                if (!otherEdgeTypes.isEmpty()) {
//                    persist(otherEdgeTypes, tx);
//                    mutator.flush();
//                    //Register new keys with indexprovider
//                    for (InternalRelationType itype : otherEdgeTypes.keySet()) {
//                        if (itype.isPropertyKey() && itype.isNew())
//                            indexSerializer.newPropertyKey((TitanKey) itype, mutator);
//                    }
//                }
//
//                if (!mutations.isEmpty()) persist(mutations, tx);
//                mutator.commit();
//                return Boolean.TRUE;
//            }
//
//            @Override
//            public String toString() {
//                return "PersistingTransaction";
//            }
//        };
//        BackendOperation.execute(persist, maxWriteRetryAttempts, retryStorageWaitTime);
    }



}
