package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.graphdb.relations.InternalRelation;

public class ArrayAdjListFactory implements AdjacencyListFactory {

	private enum Extension { Typed, UniformSet };
	
	private static final float defaultUpdateFactor = 2.0f;
	private static final int defaultInitialCapacity=5;
	private static final int defaultMaxCapacity=20;
	
	private final int initialCapacity;
	private final int maxCapacity;
	private final float updateFactor;
	private final Extension extension;
	
	public static final ArrayAdjListFactory defaultInstance = new ArrayAdjListFactory(Extension.Typed);
	public static final ArrayAdjListFactory uniformSetInstance = new ArrayAdjListFactory(Extension.UniformSet);
	
	private ArrayAdjListFactory(Extension ext) {
		initialCapacity=defaultInitialCapacity;
		maxCapacity=defaultMaxCapacity;
		updateFactor = defaultUpdateFactor;
		extension = ext;
	}
	
	@Override
	public AdjacencyList emptyList() {
		return new ArrayAdjacencyList(this);
	}

	@Override
	public AdjacencyList extend(AdjacencyList list, InternalRelation newEdge, ModificationStatus status) {
		AdjacencyList newadj = null;
		switch(extension) {
		case Typed: 
			newadj = new TypedAdjacencyList(TypedAdjListFactory.defaultInstance,list);
			break;
		case UniformSet:
			newadj = new SetAdjacencyList(SetAdjListFactory.uniformTypeInstance,list);
			break;
		default: throw new IllegalArgumentException("Unexpected input: " +extension);
		}
		newadj.addEdge(newEdge,status);
		return newadj;
	}
	
	int getInitialCapacity() {
		return initialCapacity;
	}

	int getMaxCapacity() {
		return maxCapacity;
	}
	
	int updateCapacity(int oldCapacity) {
		return (int)Math.round(oldCapacity*updateFactor);
	}
}
