package com.thinkaurelius.titan.graphdb.edges.persist;

import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.adjacencylist.ModificationStatus;
import com.thinkaurelius.titan.graphdb.edges.InlineEdge;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edges.LabeledBinaryRelationship;
import com.thinkaurelius.titan.graphdb.entitystatus.BasicEntity;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public class PersistLabeledBinaryRelationship extends LabeledBinaryRelationship {

	protected BasicEntity entity;
	
	public PersistLabeledBinaryRelationship(RelationshipType type,
			InternalNode start, InternalNode end, GraphTx tx, AdjacencyListFactory adjList) {
		super(type, start, end, tx, adjList);
		entity = new BasicEntity();
	}
	
	public PersistLabeledBinaryRelationship(RelationshipType type,
			InternalNode start, InternalNode end, GraphTx tx, AdjacencyListFactory adjList, long id) {
		super(type, start, end, tx, adjList);
		entity = new BasicEntity(id);
	}

	
	@Override
	public PersistLabeledBinaryRelationship clone() {
		assert isLoaded() && hasID();
		PersistLabeledBinaryRelationship clone = new PersistLabeledBinaryRelationship(
				getRelationshipType(),getNodeAt(0),getNodeAt(1),tx,outEdges.getFactory(),getID());
		for (InternalEdge rel : outEdges.getEdges()) {
			clone.outEdges.addEdge(((InlineEdge)rel).clone(),ModificationStatus.none);
		}
		return clone;
	}
	
	/* ---------------------------------------------------------------
	 * State Management
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public void forceDelete() {
		super.forceDelete();
		entity.delete();
	}
	
	private synchronized void updateLabeledEdge() {
		if (isLoaded()) {
			//Copy edge for deletion
			PersistLabeledBinaryRelationship clone = clone();
			tx.deletedEdge(clone);
			clone.entity.delete();
			entity.resetNew();
			tx.addedEdge(this);				
		}
	}
	
	@Override
	public void deleteEdge(InternalEdge e) {
		assert isAccessible() && e.isIncidentOn(this) && e.getDirection(this)==Direction.Out;
		if (outEdges.containsEdge(e)) {
			updateLabeledEdge();
			super.deleteEdge(e);
		}
	}
	
	@Override
	public boolean addEdge(InternalEdge e, boolean isNew) {
		if (isNew && outEdges!=null && !outEdges.containsEdge(e)) {
			updateLabeledEdge();
		}
		return super.addEdge(e, isNew);
	}
	

	/* ---------------------------------------------------------------
	 * ID Management
	 * ---------------------------------------------------------------
	 */

	@Override
	public long getID() {
		return entity.getID();
	}



	@Override
	public boolean hasID() {
		return entity.hasID();
	}
	
	
	@Override
	public void setID(long id) {
		entity.setID(id);
	}
	

	/* ---------------------------------------------------------------
	 * LifeCycle Management
	 * ---------------------------------------------------------------
	 */

	@Override
	public boolean isModified() {
		return entity.isModified();
	}


	@Override
	public boolean isAvailable() {
		return entity.isAvailable();
	}


	@Override
	public boolean isDeleted() {
		return entity.isDeleted();
	}

	@Override
	public boolean isLoaded() {
		return entity.isLoaded();
	}


	@Override
	public boolean isNew() {
		return entity.isNew();
	}

	@Override
	public boolean isReferenceNode() {
		return entity.isReferenceNode();
	}

	
	

}
