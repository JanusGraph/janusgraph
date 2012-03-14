package com.thinkaurelius.titan.graphdb.vertices;

import com.thinkaurelius.titan.core.Edge;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;

import java.util.Iterator;

public class RemovableEdgeIterable<O extends Edge>
implements Iterable<O>  {

	private final Iterable<InternalEdge> iterable;
	
	public RemovableEdgeIterable(Iterable<InternalEdge> iter) {
		iterable = iter;
	}
	
	@Override
	public Iterator<O> iterator() {
		return new RemovableEdgeIterator<O>(iterable.iterator());
	}
	
}
