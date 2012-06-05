package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.graphdb.relations.InternalRelation;

public class TypedAdjListFactory implements AdjacencyListFactory {

	private static final int expectedNumEdgeTypes = 5;

	public static final TypedAdjListFactory defaultInstance = new TypedAdjListFactory();
	
	private TypedAdjListFactory() {
		
	}
	
	@Override
	public AdjacencyList emptyList() {
		return new TypedAdjacencyList(this);
	}

	@Override
	public AdjacencyList extend(AdjacencyList list, InternalRelation newEdge,
			ModificationStatus status) {
		throw new UnsupportedOperationException("There is no extension for typed adjacency lists");
	}
	
	AdjacencyList getEmptyTypeAdjList() {
		return new ArrayAdjacencyList(ArrayAdjListFactory.uniformSetInstance);
	}
	
	int getInitialCapacity() {
		return expectedNumEdgeTypes;
	}
	
	int getConcurrencyLevel() {
		return 1;
	}
	
	float getLoadFactor() {
		return 0.75f;
	}

}
