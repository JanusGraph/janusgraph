package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.graphdb.relations.InternalRelation;

public class SetAdjListFactory implements AdjacencyListFactory {

	private static final int expectedNumEdges = 50;
	
	public static final SetAdjListFactory defaultInstance = new SetAdjListFactory(false);
	public static final SetAdjListFactory uniformTypeInstance = new SetAdjListFactory(true);
	
	private final boolean uniformType;
	
	private SetAdjListFactory(boolean uniformType) {
		this.uniformType = uniformType;
	}
	
	@Override
	public AdjacencyList emptyList() {
		return new SetAdjacencyList(this);
	}

	@Override
	public AdjacencyList extend(AdjacencyList list, InternalRelation newEdge,
			ModificationStatus status) {
		throw new UnsupportedOperationException("There is no extension for set adjacency lists");
	}

	boolean isUniformTyped() {
		return uniformType;
	}
	
	int getInitialCapacity() {
		return expectedNumEdges;
	}
	
	int getConcurrencyLevel() {
		return 1;
	}
	
	float getLoadFactor() {
		return 0.75f;
	}

}
