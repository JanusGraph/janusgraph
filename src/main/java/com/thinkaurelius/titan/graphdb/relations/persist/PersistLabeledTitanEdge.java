package com.thinkaurelius.titan.graphdb.relations.persist;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.adjacencylist.ModificationStatus;
import com.thinkaurelius.titan.graphdb.relations.InlineRelation;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.LabeledTitanEdge;
import com.thinkaurelius.titan.graphdb.entitystatus.BasicElement;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;

public class PersistLabeledTitanEdge extends LabeledTitanEdge {

	protected BasicElement entity;
	
	public PersistLabeledTitanEdge(TitanLabel type,
                                   InternalTitanVertex start, InternalTitanVertex end, InternalTitanTransaction tx, AdjacencyListFactory adjList) {
		super(type, start, end, tx, adjList);
		entity = new BasicElement();
	}
	
	public PersistLabeledTitanEdge(TitanLabel type,
                                   InternalTitanVertex start, InternalTitanVertex end, InternalTitanTransaction tx, AdjacencyListFactory adjList, long id) {
		super(type, start, end, tx, adjList);
		entity = new BasicElement(id);
	}

	
	@Override
	public PersistLabeledTitanEdge clone() {
		assert isLoaded() && hasID();
		PersistLabeledTitanEdge clone = new PersistLabeledTitanEdge(
				getTitanLabel(), getVertex(0), getVertex(1),tx,outEdges.getFactory(), getID());
		for (InternalRelation rel : outEdges.getEdges()) {
			clone.outEdges.addEdge(((InlineRelation)rel).clone(),ModificationStatus.none);
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
		entity.remove();
	}
	
	private synchronized void updateLabeledEdge() {
		if (isLoaded()) {
			//Copy edge for deletion
			PersistLabeledTitanEdge clone = clone();
			tx.deletedRelation(clone);
			clone.entity.remove();
			entity.resetNew();
			tx.addedRelation(this);
		}
	}
	
	@Override
	public void removeRelation(InternalRelation e) {
		assert isAccessible() && e.isIncidentOn(this) && e.getDirection(this)== Direction.OUT;
		if (outEdges.containsEdge(e)) {
			updateLabeledEdge();
			super.removeRelation(e);
		}
	}
	
	@Override
	public boolean addRelation(InternalRelation e, boolean isNew) {
		if (isNew && outEdges!=null && !outEdges.containsEdge(e)) {
			updateLabeledEdge();
		}
		return super.addRelation(e, isNew);
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
	public boolean isRemoved() {
		return entity.isRemoved();
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
	public boolean isReferenceVertex() {
		return entity.isReferenceVertex();
	}

	
	

}
