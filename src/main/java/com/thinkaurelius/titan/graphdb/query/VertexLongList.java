package com.thinkaurelius.titan.graphdb.query;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.InvalidElementException;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

import java.util.Iterator;

public class VertexLongList implements VertexListInternal {

	private final InternalTitanTransaction tx;
	private final AbstractLongList vertices;
	private boolean sorted;
	
	public VertexLongList(InternalTitanTransaction tx) {
		this(tx,new LongArrayList(),false);
	}

    public VertexLongList(InternalTitanTransaction tx, AbstractLongList vertices) {
        this(tx,vertices,false);
    }

	private VertexLongList(InternalTitanTransaction tx, AbstractLongList vertices, boolean sorted) {
		this.tx = tx;
		this.vertices = vertices;
		this.sorted = sorted;
	}

	@Override
	public void add(TitanVertex n) {
		if (!n.hasID()) throw new InvalidElementException("Neighboring node does not have an id",n);
		if (sorted) Preconditions.checkArgument(n.getID()>=vertices.get(vertices.size()-1),"Nodes must be inserted in sorted order");
		vertices.add(n.getID());
	}

	@Override
	public long getID(int pos) {
		return vertices.get(pos);
	}

	@Override
	public AbstractLongList getIDs() {
		return vertices;
	}

	@Override
	public TitanVertex get(int pos) {
		return tx.getExistingVertex(getID(pos));
	}

	@Override
	public void sort() {
		if (sorted) return;
        vertices.sort();
        sorted = true;
	}

	@Override
	public int size() {
		return vertices.size();
	}
    
    @Override
    public void addAll(VertexList nodelist) {
        Preconditions.checkArgument(nodelist instanceof VertexLongList,"Only supporting union of identical lists.");
        VertexLongList other = (VertexLongList)nodelist;
        sorted=false;
        vertices.addAllOfFromTo(other.vertices,0,other.vertices.size()-1);
    }

	@Override
	public Iterator<TitanVertex> iterator() {
		return new Iterator<TitanVertex>() {

			private int pos=-1;
			
			@Override
			public boolean hasNext() {
				return (pos+1)<size();
			}

			@Override
			public TitanVertex next() {
				pos++;
				return get(pos);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("Nodes cannot be removed from neighborhood list");
			}
			
		};
	}
	
}
