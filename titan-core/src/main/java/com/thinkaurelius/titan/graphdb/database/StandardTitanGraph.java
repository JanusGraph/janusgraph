package com.thinkaurelius.titan.graphdb.database;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.lucene.IndexInformation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.thinkaurelius.titan.graphdb.blueprints.TitanFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.indexing.StandardIndexInformation;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.StandardElementQuery;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Features;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StandardTitanGraph extends TitanBlueprintsGraph {

    private static final int defaultOutputCapacity = 128;

    private static final Logger log =
            LoggerFactory.getLogger(StandardTitanGraph.class);

    private final GraphDatabaseConfiguration config;
    private final IDManager idManager;
    private final VertexIDAssigner idAssigner;
    private boolean isOpen;

    private final Backend backend;

    private final int maxWriteRetryAttempts;
    private final int retryStorageWaitTime;

    private final IndexSerializer indexSerializer;
    private final EdgeSerializer edgeSerializer;
    private final Serializer serializer;


    public StandardTitanGraph(GraphDatabaseConfiguration configuration) {
        this.config = configuration;
        this.backend = configuration.getBackend();
        this.maxWriteRetryAttempts = config.getWriteAttempts();
        this.retryStorageWaitTime = config.getStorageWaittime();


        this.idAssigner = config.getIDAssigner(backend);
        this.idManager = idAssigner.getIDManager();

        this.serializer = config.getSerializer();
        this.indexSerializer = new IndexSerializer(this.serializer,this.backend.getIndexInformation());
        this.edgeSerializer = new EdgeSerializer(this.serializer,this.idManager);
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
        return startTransaction(new TransactionConfig(config,false));
    }

    @Override
    public TitanTransaction newThreadBoundTransaction() {
        return startTransaction(new TransactionConfig(config,true));
    }

    public StandardTitanTx startTransaction(TransactionConfig configuration) {
        try {
            return new StandardTitanTx(this, configuration, backend.beginTransaction());
        } catch (StorageException e) {
            throw new TitanException("Could not start new transaction", e);
        }
    }

    public IndexInformation getIndexInformation(String indexName) {
        if (Titan.Token.STANDARD_INDEX.equals(indexName)) {
            return StandardIndexInformation.INSTANCE;
        } else {
            IndexInformation indexinfo = backend.getIndexInformation().get(indexName);
            Preconditions.checkArgument(indexinfo!=null,"Index is unknown or not configured: %s",indexName);
            return indexinfo;
        }
    }

    public IDInspector getIDInspector() {
        return idManager;
    }

    public EdgeSerializer getEdgeSerializer() {
        return edgeSerializer;
    }

    public GraphDatabaseConfiguration getConfiguration() {
        return config;
    }

    // ################### READ #########################

    public boolean containsVertexID(long id, BackendTransaction tx) {
        log.trace("Checking node existence for {}", id);
        return tx.edgeStoreContainsKey(IDHandler.getKey(id));
    }

    public RecordIterator<Long> getVertexIDs(final BackendTransaction tx) {
        if (!backend.getStoreFeatures().supportsScan())
            throw new UnsupportedOperationException("The configured storage backend does not support global graph operations - use Faunus instead");
        final RecordIterator<ByteBuffer> keyiter = tx.edgeStoreKeys();
        return new RecordIterator<Long>() {

            @Override
            public boolean hasNext() throws StorageException {
                return keyiter.hasNext();
            }

            @Override
            public Long next() throws StorageException {
                return IDHandler.getKeyID(keyiter.next());
            }

            @Override
            public void close() throws StorageException {
                keyiter.close();
            }
        };
    }


    public List<Object> elementQuery(StandardElementQuery query, BackendTransaction tx) {
        return indexSerializer.query(query,tx);
    }

    public List<Entry> edgeQuery(long vid, SliceQuery query, BackendTransaction tx) {
        Preconditions.checkArgument(vid>0);
        return tx.edgeStoreQuery(new KeySliceQuery(IDHandler.getKey(vid),query));
    }



    // ################### WRITE #########################

    public void assignID(InternalVertex vertex) {
        idAssigner.assignID(vertex);
    }

    public void save(final Collection<InternalRelation> addedRelations,
                     final Collection<InternalRelation> deletedRelations, final StandardTitanTx tx) throws StorageException {
        //Setup
        log.debug("Saving transaction. Added {}, removed {}", addedRelations.size(), deletedRelations.size());

        final BackendTransaction mutator = tx.getTxHandle();
        final boolean acquireLocks = tx.getConfiguration().hasAcquireLocks();

        //1. Assign TitanVertex IDs
        if (!tx.getConfiguration().hasAssignIDsImmediately())
            idAssigner.assignIDs(addedRelations);

        for (int saveAttempt = 0; saveAttempt < maxWriteRetryAttempts; saveAttempt++) {
//        while (true) { //Indefinite loop, broken if no exception occurs, otherwise retried or failed immediately
            try {
                //2. Collect deleted edges
                ListMultimap<InternalVertex, InternalRelation> mutations = ArrayListMultimap.create();
                if (deletedRelations != null && !deletedRelations.isEmpty()) {
                    for (InternalRelation del : deletedRelations) {
                        Preconditions.checkArgument(del.isRemoved());
                        for (int pos = 0; pos < del.getLen(); pos++) {
                            InternalVertex vertex = del.getVertex(pos);
                            mutations.put(vertex, del);
                            Direction dir = EdgeDirection.fromPosition(pos);
                            if (acquireLocks && del.getType().isUnique(dir) &&
                                    ((InternalType) del.getType()).uniqueLock(dir)) {
                                Entry entry = edgeSerializer.writeRelation(del,pos,tx);
                                mutator.acquireEdgeLock(IDHandler.getKey(vertex.getID()), entry.getColumn(), entry.getValue());
                            }
                        }
                        //Update Indexes
                        if (del.isProperty()) {
                            if (acquireLocks) indexSerializer.lockKeyedProperty((TitanProperty) del,mutator);
                            indexSerializer.removeProperty((TitanProperty) del,mutator);
                        } else if (del.isEdge()) {
                            indexSerializer.removeEdge(del,mutator);
                        }

                    }
                }

                ListMultimap<InternalType, InternalRelation> otherEdgeTypes = null;

                //3. Sort Added Edges
                for (InternalRelation relation : addedRelations) {
                    assert relation.isNew();

                    TitanType type = relation.getType();

                    //Give special treatment to edge type definitions
                    if (SystemTypeManager.prepersistedSystemTypes.contains(type)) {
                        InternalType itype = (InternalType) relation.getVertex(0);
                        if (otherEdgeTypes == null) otherEdgeTypes = ArrayListMultimap.create();
                        otherEdgeTypes.put(itype, relation);
                    } else { //STANDARD TitanRelation
                        assert (relation.getArity() == 1 && relation.isProperty()) || (relation.getArity() == 2 && relation.isEdge());
                        for (int pos = 0; pos < relation.getLen(); pos++) {
                            InternalVertex node = relation.getVertex(pos);
                            mutations.put(node, relation);
                            Direction dir = EdgeDirection.fromPosition(pos);
                            if (acquireLocks && relation.getType().isUnique(dir) && !node.isNew()
                                    && ((InternalType) relation.getType()).uniqueLock(dir)) {
                                Entry entry = edgeSerializer.writeRelation(relation, pos, false, tx);
                                mutator.acquireEdgeLock(IDHandler.getKey(node.getID()), entry.getColumn(), null);
                            }
                        }
                    }
                    //Update Indexes
                    if (relation.isProperty()) {
                        if (acquireLocks) indexSerializer.lockKeyedProperty((TitanProperty) relation,mutator);
                    }

                }

                //3. Persist
                if (otherEdgeTypes != null) persist(otherEdgeTypes, tx);
                mutator.flush();

                //Register new keys with indexprovider
                for (InternalType itype : otherEdgeTypes.keySet()) {
                    if (itype.isPropertyKey() && itype.isNew()) indexSerializer.newPropertyKey((TitanKey)itype,mutator);
                }

                if (!mutations.isEmpty()) persist(mutations, tx);


                //Successfully completed - return to break out of loop
                break;
            } catch (Throwable e) {
                if (e instanceof TemporaryStorageException) {
                    if (saveAttempt < maxWriteRetryAttempts - 1) BackendTransaction.temporaryStorageException(e,retryStorageWaitTime);
                    else
                        throw new PermanentStorageException("Tried committing " + maxWriteRetryAttempts + " times on temporary exception without success", e);
                } else if (e instanceof StorageException) {
                    throw (StorageException) e;
                } else {
                    throw new PermanentStorageException("Unidentified exception occurred during persistence", e);
                }
            }
        }
    }


    private <V extends InternalVertex> void persist(ListMultimap<V, InternalRelation> mutatedEdges,
                                                    StandardTitanTx tx) throws StorageException {
        assert mutatedEdges != null && !mutatedEdges.isEmpty();

        Collection<V> vertices = mutatedEdges.keySet();
//		if (sortNodes) {
//			List<V> sortedvertices = new ArrayList<V>(vertices);
//			Collections.sort(sortedvertices);
//			vertices=sortedvertices;
//		}
        BackendTransaction mutator = tx.getTxHandle();
        for (V vertex : vertices) {
            List<InternalRelation> edges = mutatedEdges.get(vertex);
            List<Entry> additions = new ArrayList<Entry>(edges.size());
            List<ByteBuffer> deletions = new ArrayList<ByteBuffer>(Math.max(10, edges.size() / 10));
            for (InternalRelation edge : edges) {
                for (int pos=0;pos<edge.getArity();pos++) {
                    if (edge.getVertex(pos).equals(vertex)) {
                        if (edge.isRemoved()) {
                            deletions.add(edgeSerializer.writeRelation(edge, pos, false, tx).getColumn());
                        } else {
                            assert edge.isNew();
                            if (edge.isProperty()) {
                                indexSerializer.addProperty((TitanProperty) edge,mutator);
                            } else {
                                indexSerializer.addEdge(edge, mutator);
                            }
                            additions.add(edgeSerializer.writeRelation(edge, pos, tx));
                        }
                    }
                }
            }
            mutator.mutateEdges(IDHandler.getKey(vertex.getID()), additions, deletions);
        }

    }

}
