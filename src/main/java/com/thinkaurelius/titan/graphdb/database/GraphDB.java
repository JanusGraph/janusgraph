package com.thinkaurelius.titan.graphdb.database;

import cern.colt.list.AbstractLongList;
import com.thinkaurelius.titan.core.GraphDatabase;
import com.thinkaurelius.titan.core.GraphTransactionConfig;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

import java.util.Collection;

public interface GraphDB extends GraphDatabase {


	IDInspector getIDInspector();
	
	// ######## Node / Edge Loading  ############
	
	boolean containsNodeID(long id, GraphTx tx);

    public void assignID(InternalNode node);

	void loadEdges(InternalEdgeQuery query, GraphTx tx);
	
	public AbstractLongList getRawNeighborhood(InternalEdgeQuery query, GraphTx tx);
	
	public long[] indexRetrieval(Object value, PropertyType pt, GraphTx tx);

	public GraphTx startTransaction(GraphTransactionConfig configuration);
	
	// ######## Node Operations  ############
	
	boolean save(Collection<InternalEdge> addedEdges, Collection<InternalEdge> deletedEdges, GraphTx tx) throws GraphStorageException ;

	
}
