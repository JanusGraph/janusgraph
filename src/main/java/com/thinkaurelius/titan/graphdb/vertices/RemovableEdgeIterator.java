package com.thinkaurelius.titan.graphdb.vertices;

import com.thinkaurelius.titan.core.Edge;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;

import java.util.Iterator;

public class RemovableEdgeIterator <O extends Edge>
implements Iterator<O> {

	
	private final Iterator<InternalEdge> iterator;
	private InternalEdge current;
	
	public RemovableEdgeIterator(Iterator<InternalEdge> iter) {
		iterator = iter;
		current = null;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@SuppressWarnings("unchecked")
	@Override
	public O next() {
		current = iterator.next();
		return (O)current;
	}

	@Override
	public void remove() {
		assert current!=null;
        //iterator.remove();
		current.delete();
	}

	
}
