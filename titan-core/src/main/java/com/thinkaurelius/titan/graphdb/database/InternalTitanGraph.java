package com.thinkaurelius.titan.graphdb.database;

import cern.colt.list.AbstractLongList;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.query.AtomicQuery;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Features;

import java.util.Collection;

public interface InternalTitanGraph extends TitanGraph {

    public Features getFeatures();

    IDInspector getIDInspector();

    // ######## TitanVertex / TitanRelation Loading  ############

    boolean containsVertexID(long id, InternalTitanTransaction tx);

    public void assignID(InternalTitanVertex vertex);

    public boolean isReferenceVertexID(long vertexid);

    void loadRelations(AtomicQuery query, InternalTitanTransaction tx);

    public AbstractLongList getRawNeighborhood(AtomicQuery query, InternalTitanTransaction tx);

    public long[] indexRetrieval(Object value, TitanKey key, InternalTitanTransaction tx);

    public RecordIterator<Long> getVertexIDs(InternalTitanTransaction tx);

    public InternalTitanTransaction startTransaction(TransactionConfig configuration);


    // ######## TitanVertex Operations  ############

    void save(Collection<InternalRelation> addedRelations, Collection<InternalRelation> deletedRelations, InternalTitanTransaction tx) throws StorageException;


}
