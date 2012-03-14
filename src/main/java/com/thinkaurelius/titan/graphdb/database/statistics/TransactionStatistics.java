package com.thinkaurelius.titan.graphdb.database.statistics;

import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import com.thinkaurelius.titan.util.datastructures.LongCounter;

import java.util.HashMap;
import java.util.Map;

public class TransactionStatistics {

	private long deltaNodes = 0;
	private long deltaEdgeTypes = 0;
	private Map<EdgeType,LongCounter> deltaET = new HashMap<EdgeType,LongCounter>();
	
	public void addedEdge(InternalEdge edge) {
		incrementET(edge.getEdgeType(),1);
	}
	
	public void removedEdge(InternalEdge edge) {
		incrementET(edge.getEdgeType(),-1);		
	}
	
	public void addedNode(InternalNode node) {
		if (node instanceof InternalEdgeType) {
			deltaEdgeTypes++;
		} else {
			deltaNodes++;
		}
	}
	
	public void removedNode(InternalNode node) {
		deltaNodes--;
	}
	
	public long getNodeDelta() {
		return deltaNodes;
	}
	
	public long getEdgeTypeDelta() {
		return deltaEdgeTypes;
	}
	
	public  Map<EdgeType,LongCounter> getDeltaEdgeTypes() {
		return deltaET;
	}
	
	private void incrementET(EdgeType et, int delta) {
		LongCounter lc = deltaET.get(et);
		if (lc==null) {
			lc = new LongCounter(0);
			deltaET.put(et, lc);
		}
		lc.increment(delta);
	}
	
}
