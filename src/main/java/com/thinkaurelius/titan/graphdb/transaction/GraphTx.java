package com.thinkaurelius.titan.graphdb.transaction;

import cern.colt.list.AbstractLongList;
import com.thinkaurelius.titan.core.GraphTransaction;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edgequery.Interval;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edges.factory.EdgeLoader;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public interface GraphTx extends GraphTransaction {
		
	/**
	 * Returns the storage backend transaction handle issued to this transaction
	 * @return The storage backend transaction handle issued to this transaction
	 */
	TransactionHandle getTxHandle();
	
	
	/**
	 * Returns a proxy to load edges into this transaction
	 * @return Proxy to load edges into this transaction
	 */
	EdgeLoader getEdgeFactory();
	
	/**
	 * Starts a new edge query on the specified node
	 * 
	 * @param n Node for which to create a new edge query
	 * @return New edge query
	 */
	InternalEdgeQuery makeEdgeQuery(InternalNode n);

	
	// ######## Node / Edge Loading  ############
	
	/**
	 * Called to load all edges into the transaction that are needed to answer the provided edge query
	 * 
	 * @param query Edge query for which to load all edges
	 */
	void loadEdges(InternalEdgeQuery query);
	
	/**
	 * Retrieves the node ids for the neighboring nodes addresed in the given neighborhood query
	 * 
	 * @param query Neighborhood query for which to retrieve neighboring node ids
	 * @return Node ids of neighbors
	 */
	AbstractLongList getRawNeighborhood(InternalEdgeQuery query);
			
	/**
	 * Notifies the transaction that the specified edge has been deleted so
	 * that the change can be persisted.
	 * 
	 * @param edge Deleted edge
	 */
	void deletedEdge(InternalEdge edge);
	
	/**
	 * Notifies the transaction that the specified edge has been added so
	 * that the change can be persisted.
	 * 
	 * This method automatically calls {@link #loadedEdge(com.thinkaurelius.titan.graphdb.edges.InternalEdge)}.
	 * 
	 * @param edge Added edge
	 */
	void addedEdge(InternalEdge edge);
	
	/**
	 * Notifies the transaction that the specified edge has been loaded from disk
	 * into this transaction so that any affected in-memory data structures can be
	 * updated.
	 * 
	 * @param edge Loaded edge
	 */
	void loadedEdge(InternalEdge edge);
	
	/**
	 * Checks whether the specified edge has been deleted in this transaction
	 * 
	 * @param e Edge to check for deletion
	 * @return true, if the edge has been deleted, else false
	 */
	public boolean isDeletedEdge(InternalEdge e);
	
	// ######## Node / Edge Loading  ############
	
	/**
	 * Returns the node for an existing node id
	 * @param id Node id
	 * @return Node associated with the specified id
	 */
	InternalNode getExistingNode(long id);

    /**
     * Whenever a new entity gets created within the current transaction,
     * it has to be registered with the transaction using this method.
     *
     * @param n Newly created entity
     */
    void registerNewEntity(InternalNode n);


    /**
     * Deletes nodes from transaction.
     *
     * @param n Deleted node
     */
    public void deleteNode(InternalNode n);

    /**
     * Retrieves all ids for nodes which have an incident property of the given type with the specified attribute value
     *
     * The given property type must have an hasIndex defined for this retrieval to succeed.
     *
     * @param type Property type for which to retrieve nodes
     * @param attribute Attribute value for which to retrieve nodes
     * @return	All ids for nodes which have an incident property of the given type with the specified attribute value
     * @throws	IllegalArgumentException if the property type is not indexed.
     * @see com.thinkaurelius.titan.graphdb.edgequery.Interval
     */
    public long[] getNodeIDsByAttributeFromDisk(PropertyType type, Object attribute);

	


}
