package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.thinkaurelius.titan.graphdb.relations.InternalRelation;

public enum InitialAdjListFactory implements AdjacencyListFactory {

	BasicFactory {

		@Override
		public AdjacencyList emptyList() {
			return basicInitial;
		}


		@Override
		public AdjacencyList extend(AdjacencyList list, InternalRelation newEdge,
				ModificationStatus status) {
			AdjacencyList newList = new ArrayAdjacencyList(ArrayAdjListFactory.defaultInstance);
			newList.addEdge(newEdge,status);
			return newList;
		}
	},
	
	EmptyFactory {
		
		@Override
		public AdjacencyList emptyList() {
			return emptyInitial;
		}


		@Override
		public AdjacencyList extend(AdjacencyList list, InternalRelation newEdge,
				ModificationStatus status) {
			throw new IllegalStateException("Cannot add to emptied adjacency list");
		}
		
	};
	
	
	private static final AdjacencyList basicInitial = new InitialAdjacencyList(BasicFactory);

	private static final AdjacencyList emptyInitial = new InitialAdjacencyList(EmptyFactory);

	/*
	 * These two abstract declarations are a workaround for bug 6724345
	 * which was fixed in JDK 7 b39:
	 * 
	 * http://bugs.sun.com/view_bug.do?bug_id=6724345
	 */
	public abstract AdjacencyList emptyList();
	public abstract AdjacencyList extend(AdjacencyList list, 
			InternalRelation newEdge, ModificationStatus status);
	
}
