package com.thinkaurelius.titan.graphdb.vertices;

import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.query.AtomicQuery;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.entitystatus.BasicElement;
import com.thinkaurelius.titan.graphdb.loadingstatus.LoadingStatus;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public class PersistStandardTitanVertex extends StandardTitanVertex {

	protected BasicElement entity;
	private LoadingStatus loading;
	
	public PersistStandardTitanVertex(InternalTitanTransaction g, AdjacencyListFactory adjList) {
		super(g,adjList);
		entity = new BasicElement();
		loading = LoadingStatus.AllLoaded;
	}
	
	public PersistStandardTitanVertex(InternalTitanTransaction g, AdjacencyListFactory adjList, long id) {
		super(g,adjList);
		entity = new BasicElement(id);
		loading = LoadingStatus.NothingLoaded;
	}

	/* ---------------------------------------------------------------
	 * State Management
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public synchronized void remove() {
		super.remove();
		entity.remove();
		loading = LoadingStatus.NothingLoaded;
	}
	
	@Override
	public void removeRelation(InternalRelation e) {
		super.removeRelation(e);
		if (e.isIncidentOn(this)) entity.modified();
	}
	
	@Override
	public boolean addRelation(InternalRelation e, boolean isNew) {
		if (super.addRelation(e, isNew)) {
			if (isNew) {
				assert e.isNew();
				entity.modified();
			}
			return true;
		} else return false;
	}
	
	@Override
	public void loadedEdges(AtomicQuery query) {
		loading = loading.loadedEdges(query);
	}

	@Override
	public boolean hasLoadedEdges(AtomicQuery query) {
		return loading.hasLoadedEdges(query);
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
