package com.thinkaurelius.titan.graphdb.vertices;

import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.graphdb.edges.InternalRelation;

import java.util.Iterator;

public class RemovableEdgeIterable<O extends TitanRelation>
implements Iterable<O>  {

	private final Iterable<InternalRelation> iterable;
	
	public RemovableEdgeIterable(Iterable<InternalRelation> iter) {
		iterable = iter;
	}
	
	@Override
	public Iterator<O> iterator() {
		return new RemovableEdgeIterator<O>(iterable.iterator());
	}
	
}
