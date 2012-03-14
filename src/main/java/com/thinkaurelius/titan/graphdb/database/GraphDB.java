package com.thinkaurelius.titan.graphdb.database;

import cern.colt.list.AbstractLongList;
import com.thinkaurelius.titan.core.GraphDatabase;
import com.thinkaurelius.titan.core.GraphTransactionConfig;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.sendquery.QuerySender;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

import java.util.Collection;

public interface GraphDB extends GraphDatabase {


	IDInspector getIDInspector();
	
	// ######## Node / Edge Loading  ############
	
	boolean containsNodeID(long id, GraphTx tx);
	

	void loadEdges(InternalEdgeQuery query, GraphTx tx);
	
	public AbstractLongList getRawNeighborhood(InternalEdgeQuery query, GraphTx tx);
	
//	public Multimap<Long,Object[]> getQualifiedNeighborhood(InternalNeighborhoodQuery query, GraphTx tx);
	
	public long[] indexRetrieval(Interval<?> interval, PropertyType pt, GraphTx tx);
	
	public AbstractLongList getAllNodeIDs(long startRange, long endRange);
	
	public GraphTx startTransaction(GraphTransactionConfig configuration, QuerySender sender);
	
	// ######## Node Operations  ############
	
	void flush(Collection<InternalEdge> addedEdges, Collection<InternalEdge> deletedEdges, GraphTx tx) throws GraphStorageException ;
	
	boolean save(Collection<InternalEdge> addedEdges, Collection<InternalEdge> deletedEdges, GraphTx tx) throws GraphStorageException ;

	
}
