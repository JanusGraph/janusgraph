package com.thinkaurelius.titan.graphdb.vertices;

import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.entitystatus.BasicEntity;
import com.thinkaurelius.titan.graphdb.loadingstatus.LoadingStatus;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

public class PersistDualNode extends DualNode {

	protected BasicEntity entity;
	private LoadingStatus loading;
	
	public PersistDualNode(GraphTx g, AdjacencyListFactory adjList) {
		super(g,adjList);
		entity = new BasicEntity();
		loading = LoadingStatus.AllLoaded;
	}
	
	public PersistDualNode(GraphTx g, AdjacencyListFactory adjList, long id) {
		super(g,adjList);
		entity = new BasicEntity(id);
		loading = LoadingStatus.NothingLoaded;
	}

	/* ---------------------------------------------------------------
	 * State Management
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public synchronized void delete() {
		super.delete();
		entity.delete();
		loading = LoadingStatus.NothingLoaded;
	}
	
	@Override
	public void deleteEdge(InternalEdge e) {
		super.deleteEdge(e);
		if (e.isIncidentOn(this)) entity.modified();
	}
	
	@Override
	public boolean addEdge(InternalEdge e, boolean isNew) {
		if (super.addEdge(e,isNew)) {
			if (isNew) {
				assert e.isNew();
				entity.modified();
			}
			return true;
		} else return false;
	}
	
	@Override
	public void loadedEdges(InternalEdgeQuery query) {
		loading = loading.loadedEdges(query);
	}

	@Override
	public boolean hasLoadedEdges(InternalEdgeQuery query) {
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

	@Override
	public String toString() {
		if (hasID()) return "Node"+getID();
		else return super.toString();
	}


}
