package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.exceptions.InvalidEdgeException;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyList;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.adjacencylist.InitialAdjListFactory;
import com.thinkaurelius.titan.graphdb.adjacencylist.ModificationStatus;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.EdgeDirection;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

public class DualNode extends UndirectedNode {

	private AdjacencyList inEdges;
	private AdjacencyList outEdges;
	
	public DualNode(GraphTx g, AdjacencyListFactory adjList) {
		super(g,adjList);
		inEdges = adjList.emptyList();
		outEdges = adjList.emptyList();
	}
	
	
	
	@Override
	public boolean addEdge(InternalEdge e, boolean isNew) {
		assert isAvailable();
		Preconditions.checkArgument(e.isIncidentOn(this), "Edge is not incident on this node!");
		if (e.isUndirected()) {
			return super.addEdge(e,isNew);
		} else {
			boolean success = false;
			boolean loadIn = false;
			ModificationStatus status = new ModificationStatus();
			if (e.getDirection(this).implies(EdgeDirection.In)) {
				loadIn = true;
				synchronized(inEdges) {
					inEdges = inEdges.addEdge(e,status);
				}
				success = status.hasChanged();
			}
			if (e.getDirection(this).implies(EdgeDirection.Out)) {
				synchronized(outEdges) {
					outEdges = outEdges.addEdge(e, e.getEdgeType().isFunctional(), status);
				}
				if (status.hasChanged()) {
					if (loadIn && !success) throw new InvalidEdgeException("Could only load one direction of loop-edge!");
					success=true;
				} else {
					if (loadIn && success) throw new InvalidEdgeException("Could only load one direction of loop-edge!");
					success=false;
				}
			}
			return success;
		}
	}
	
	@Override
	public Iterable<InternalEdge> getEdges(InternalEdgeQuery query,
			boolean loadRemaining) {
		assert isAvailable();
		if (loadRemaining) ensureLoadedEdges(query);
		
		Iterable<InternalEdge> iter=AdjacencyList.Empty;
		for (EdgeDirection dir : EdgeDirection.values()) {
			if (!query.isAllowedDirection(dir)) continue;
			Iterable<InternalEdge> siter;
			switch(dir) {
			case Out: siter = NodeUtil.getQuerySpecificIterable(outEdges, query);
				break;
			case In: siter = NodeUtil.filterLoopEdges(NodeUtil.getQuerySpecificIterable(inEdges, query),this);
				break;
			case Undirected: siter = super.getEdges(query, false);
				break;
			default: throw new AssertionError("Unrecognized direction: "+ dir);
			}
			if (iter==AdjacencyList.Empty) iter = siter;
			else if (siter!=AdjacencyList.Empty) iter = Iterables.concat(iter, siter);
		}
	
		iter = NodeUtil.filterQueryQualifications(query, iter);
		return iter;
	}
	
	
	@Override
	public void deleteEdge(InternalEdge e) {
		assert isAvailable() && e.isIncidentOn(this);
		Direction dir = e.getDirection(this);
		if (dir.implies(EdgeDirection.In)) inEdges.removeEdge(e,ModificationStatus.none); 
		if (dir.implies(EdgeDirection.Out)) outEdges.removeEdge(e,ModificationStatus.none);
		if (dir==Direction.Undirected) super.deleteEdge(e);
	}

	@Override
	public synchronized void delete() {
		super.delete();
		inEdges=InitialAdjListFactory.EmptyFactory.emptyList();
		outEdges=InitialAdjListFactory.EmptyFactory.emptyList();
	}


}
