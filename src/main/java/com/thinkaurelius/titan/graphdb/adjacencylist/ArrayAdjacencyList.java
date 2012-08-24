package com.thinkaurelius.titan.graphdb.adjacencylist;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.InvalidElementException;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ArrayAdjacencyList implements AdjacencyList {

	private static final long serialVersionUID = -2868708972683152295L;

	private final ArrayAdjListFactory factory;
	private InternalRelation[] contents;

	
	ArrayAdjacencyList(ArrayAdjListFactory factory) {
		this.factory=factory;
		contents = new InternalRelation[factory.getInitialCapacity()];
	}
	
	@Override
	public synchronized AdjacencyList addEdge(InternalRelation e, ModificationStatus status) {
		return addEdge(e,false,status);
	}


	@Override
	public synchronized AdjacencyList addEdge(InternalRelation e, boolean checkTypeUniqueness, ModificationStatus status) {
		int emptySlot = -1;
		for (int i=0;i<contents.length;i++) {
			if (contents[i]==null) {
				emptySlot=i;
				continue;
			}
			InternalRelation oth = contents[i];
			assert oth!=null;
			if (oth.equals(e)) {
				status.nochange();
				return this;
			} else if (checkTypeUniqueness && oth.getType().equals(e.getType())) {
				throw new InvalidElementException("Cannot add functional edge since an edge of that type already exists",e);
			}
		}
		if (emptySlot<0 && contents.length>=factory.getMaxCapacity()) {
			return factory.extend(this, e, status);
		} else {
			//Add internally
			if (emptySlot>=0) {
				contents[emptySlot]=e;
			} else {
				//Expand & copy
				InternalRelation[] contents2 = new InternalRelation[factory.updateCapacity(contents.length)];
				System.arraycopy(contents, 0, contents2, 0, contents.length);
				contents2[contents.length]=e;
				contents=contents2;
			}
			status.change();
			return this;
		}
	}

	@Override
	public boolean containsEdge(InternalRelation e) {
        InternalRelation[] c = contents;
		for (int i=0;i<c.length;i++) {
			if (c[i]!=null && e.equals(c[i])) return true;
		}
		return false;
	}
	

	@Override
	public boolean isEmpty() {
        InternalRelation[] c = contents;
		for (int i=0;i<c.length;i++) {
			if (c[i]!=null) return false;
		}
		return true;
	}

	@Override
	public synchronized void removeEdge(InternalRelation e, ModificationStatus status) {
		status.nochange();
		for (int i=0;i<contents.length;i++) {
			if (contents[i]!=null && e.equals(contents[i])) {
				contents[i]=null;
				status.change();
			}
		}
	}

	@Override
	public AdjacencyListFactory getFactory() {
		return factory;
	}

	

	@Override
	public Iterable<InternalRelation> getEdges() {
		return new Iterable<InternalRelation>() {

			@Override
			public Iterator<InternalRelation> iterator() {
				return new InternalIterator();
			}
			
		};
	}

	@Override
	public Iterable<InternalRelation> getEdges(final TitanType type) {
		return new Iterable<InternalRelation>() {

			@Override
			public Iterator<InternalRelation> iterator() {
				return new InternalTypeIterator(type);
			}
			
		};
	}
	

	@Override
	public Iterable<InternalRelation> getEdges(final TypeGroup group) {
		return new Iterable<InternalRelation>() {

			@Override
			public Iterator<InternalRelation> iterator() {
				return new InternalGroupIterator(group);
			}
			
		};
	}

	
	private class InternalGroupIterator extends InternalIterator {
		
		private final TypeGroup group;
		
		private InternalGroupIterator(TypeGroup group) {
			super(false);
			Preconditions.checkNotNull(group);
			this.group=group;
			findNext();
		}
		
		@Override
		protected boolean applies(InternalRelation edge) {
			return group.equals(edge.getType().getGroup());
		}
		
	}
	
	private class InternalTypeIterator extends InternalIterator {
		
		private final TitanType type;
		
		private InternalTypeIterator(TitanType type) {
			super(false);
			Preconditions.checkNotNull(type);
			this.type=type;
			findNext();
		}
		
		@Override
		protected boolean applies(InternalRelation edge) {
			return type.equals(edge.getType());
		}
		
	}
	
	private class InternalIterator implements Iterator<InternalRelation> {

		private InternalRelation next = null;
		private int position = -1;
		private InternalRelation last = null;
        private final InternalRelation[] contents;

		
		InternalIterator() {
			this(true);
		}
		
		InternalIterator(boolean initialize) {
            this.contents = ArrayAdjacencyList.this.contents;
			if (initialize) findNext();
		}
		

		
		protected final void findNext() {
			last = next;
			next = null;
			for (position = position+1; position<contents.length;position++) {
                next = contents[position];
				if (next!=null && applies(next)) {
					break;
				} else next = null;
			}
		}
		
		protected boolean applies(InternalRelation edge) {
			return true;
		}
		
		@Override
		public boolean hasNext() {
			return next!=null;
		}

		@Override
		public InternalRelation next() {
			if (next==null) throw new NoSuchElementException();
			InternalRelation old = next;
			findNext();
			return old;
		}

		@Override
		public void remove() {
			if (last==null) throw new NoSuchElementException();
			removeEdge(last,ModificationStatus.none);
		}
		
	}


	@Override
	public Iterator<InternalRelation> iterator() {
		return new InternalIterator();
	}


	
}
