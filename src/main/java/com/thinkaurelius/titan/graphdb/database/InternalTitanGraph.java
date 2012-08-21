package com.thinkaurelius.titan.graphdb.database;

import cern.colt.list.AbstractLongList;
import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.thinkaurelius.titan.graphdb.query.AtomicQuery;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;

import java.util.Collection;

public interface InternalTitanGraph extends TitanGraph {


	IDInspector getIDInspector();
	
	// ######## TitanVertex / TitanRelation Loading  ############
	
	boolean containsVertexID(long id, InternalTitanTransaction tx);

    public void assignID(InternalTitanVertex vertex);

    public boolean isReferenceVertexID(long vertexid);

	void loadRelations(AtomicQuery query, InternalTitanTransaction tx);
	
	public AbstractLongList getRawNeighborhood(AtomicQuery query, InternalTitanTransaction tx);
	
	public long[] indexRetrieval(Object value, TitanKey key, InternalTitanTransaction tx);

	public InternalTitanTransaction startTransaction(TransactionConfig configuration);
	
	// ######## TitanVertex Operations  ############
	
	boolean save(Collection<InternalRelation> addedRelations, Collection<InternalRelation> deletedRelations, InternalTitanTransaction tx) throws GraphStorageException;

	
}
