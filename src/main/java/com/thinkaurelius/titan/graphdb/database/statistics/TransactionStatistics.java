package com.thinkaurelius.titan.graphdb.database.statistics;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.util.datastructures.LongCounter;

import java.util.HashMap;
import java.util.Map;

public class TransactionStatistics {

	private long deltaNodes = 0;
	private long deltaEdgeTypes = 0;
	private Map<TitanType,LongCounter> deltaET = new HashMap<TitanType,LongCounter>();
	
	public void addedEdge(InternalRelation edge) {
		incrementET(edge.getType(),1);
	}
	
	public void removedEdge(InternalRelation edge) {
		incrementET(edge.getType(),-1);
	}
	
	public void addedNode(InternalTitanVertex node) {
		if (node instanceof InternalTitanType) {
			deltaEdgeTypes++;
		} else {
			deltaNodes++;
		}
	}
	
	public void removedNode(InternalTitanVertex node) {
		deltaNodes--;
	}
	
	public long getNodeDelta() {
		return deltaNodes;
	}
	
	public long getEdgeTypeDelta() {
		return deltaEdgeTypes;
	}
	
	public  Map<TitanType,LongCounter> getDeltaEdgeTypes() {
		return deltaET;
	}
	
	private void incrementET(TitanType et, int delta) {
		LongCounter lc = deltaET.get(et);
		if (lc==null) {
			lc = new LongCounter(0);
			deltaET.put(et, lc);
		}
		lc.increment(delta);
	}
	
}
