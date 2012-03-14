package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyList;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyListFactory;
import com.thinkaurelius.titan.graphdb.adjacencylist.InitialAdjListFactory;
import com.thinkaurelius.titan.graphdb.adjacencylist.ModificationStatus;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.EdgeDirection;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

public class UndirectedNode extends AbstractNode {

	private AdjacencyList edges;
	
	public UndirectedNode(GraphTx g, AdjacencyListFactory adjList) {
		super(g);
		edges = adjList.emptyList();
	}
	
	@Override
	public boolean addEdge(InternalEdge e, boolean isNew) {
		assert isAccessible();
		Preconditions.checkArgument(e.isIncidentOn(this), "Edge is not incident on this node!");
		Preconditions.checkArgument(e.isUndirected(),"This node only supports undirected edges!");
		
		ModificationStatus status = new ModificationStatus();
		synchronized(edges) {
			edges = edges.addEdge(e,status);
		}
		return status.hasChanged();
	}
	

	@Override
	public Iterable<InternalEdge> getEdges(InternalEdgeQuery query,
			boolean loadRemaining) {
		assert isAvailable();
		if (loadRemaining) ensureLoadedEdges(query);

		if (!query.isAllowedDirection(EdgeDirection.Undirected)) return AdjacencyList.Empty;
		else return NodeUtil.filterQueryQualifications(query, NodeUtil.getQuerySpecificIterable(edges, query));
	}

	
	@Override
	public void deleteEdge(InternalEdge e) {
		assert isAvailable() && e.isIncidentOn(this);
		edges.removeEdge(e,ModificationStatus.none);
	}

	@Override
	public synchronized void delete() {
		super.delete();
		edges=InitialAdjListFactory.EmptyFactory.emptyList();
	}
	
	


}
