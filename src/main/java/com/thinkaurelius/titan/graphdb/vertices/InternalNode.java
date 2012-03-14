package com.thinkaurelius.titan.graphdb.vertices;

import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.entitystatus.InternalEntity;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;


public interface InternalNode extends Node, InternalEntity
{
	
	GraphTx getTransaction();
	
	/* ---------------------------------------------------------------
	 * Incident Edge Access methods
	 * ---------------------------------------------------------------
	 */
	
	/**
	 * Returns all edges incident on this node. If filterDeleted is true
	 * only non-deleted/freed edges are returned.
	 * If loadRemaining is true, all required edges are first loaded from disk if
	 * not already in memory.
	 * @param query InternalEdgeQuery defining which edges to retrieve/return
	 * @param loadRemaining
	 * @return Iterator over all incident edges
	 */
	Iterable<InternalEdge> getEdges(InternalEdgeQuery query, boolean loadRemaining);
	
	
	/* ---------------------------------------------------------------
	 * Incident Edge Operations - Package level only
	 * ---------------------------------------------------------------
	 */

	
	/**
	 * Deleted edge e from the adjacency list of this node and updates the state of the node to reflect
	 * the modification.
	 * Note that this method tolerates the prior removal of the node and hence does not throw an exception
	 * if the edge could not actually be removed from the adjacency list. This behavior was chosen to allow
	 * edge deletion while iterating over the list of adjacent edges, in which case the edge deletion is taken
	 * care of by the iterator and only node status update needs to be executed.
	 * @param e Edge to be removed
	 */
	void deleteEdge(InternalEdge e);
	
	
	boolean addEdge(InternalEdge e, boolean isNew);
	
	/**
	 * Notifies the node that all of its edges have been loaded from disk.
     *
     * @param query Specifies which edges have been loaded, i.e. all incident edges matching the query
	 */
	void loadedEdges(InternalEdgeQuery query);


    /**
     * Checks whether edges have already been loaded into memory
     * @param query Query for which to check whether edges have been loaded already
     * @return Whether all edges asked for by the query have already been loaded into memory
     */
	boolean hasLoadedEdges(InternalEdgeQuery query);
	
	
}
