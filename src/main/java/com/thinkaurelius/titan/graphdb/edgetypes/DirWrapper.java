package com.thinkaurelius.titan.graphdb.edgetypes;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Direction;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.core.EdgeTypeGroup;
import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.edges.EdgeDirection;

public abstract class DirWrapper<V> {
	
	private final V wrapped;
	
	private DirWrapper(V wrapped) {
		this.wrapped = wrapped;
	}
	
	public abstract Direction getDirection();
	
	public boolean hasDirection() {
		return true;
	}
	
	public V get() {
		return wrapped;
	}
	
	@Override
	public int hashCode() {
		return wrapped.hashCode()*1171 + (hasDirection()?getDirection().hashCode():0);
	}
	
	@Override
	public boolean equals(Object oth) {
		if (this==oth) return true;
		else if (!getClass().isInstance(oth)) return false;
		DirWrapper<?> other = (DirWrapper<?>)oth;
		if (!get().equals(other.get())) return false;
		if (hasDirection() && other.hasDirection())  return getDirection().equals(other.getDirection());
		else if (!hasDirection() && !other.hasDirection()) return true;
		else return false;
	}
	
	public boolean hasOverlappingDirection(DirWrapper<?> other) {
		if (!this.hasDirection() || !other.hasDirection()) return true;
		Direction d1 = getDirection(), d2 = other.getDirection();
		for (EdgeDirection dir : EdgeDirection.values()) {
			if (d1.implies(dir) && d2.implies(dir)) return true;
		}
		return false;
	}

	public static final DirWrapper<RelationshipType> wrap(RelationshipType type) { 
		return wrapInternal(null,type);
	}
	
	public static final DirWrapper<RelationshipType> wrap(Direction dir, RelationshipType type) { 
		return wrapInternal(dir,type);
	}
	
	public static final DirWrapper<EdgeType> wrap(EdgeType type) { 
		return wrapInternal(null,type);
	}
	
	public static final DirWrapper<EdgeType> wrap(Direction dir, EdgeType type) { 
		return wrapInternal(dir,type);
	}
	
	public static final DirWrapper<EdgeTypeGroup> wrap(EdgeTypeGroup group) {
		return wrapInternal(null,group);
	}

	public static final DirWrapper<EdgeTypeGroup> wrap(Direction dir, EdgeTypeGroup group) {
		return wrapInternal(dir,group);
	}
	
	private static final<V> DirWrapper<V> wrapInternal(Direction dir, V wrapped) {
		Preconditions.checkNotNull(wrapped);
		if (dir==null) return new AllWrapper<V>(wrapped);
		switch(dir) {
		case Undirected:
			return new UndirectedWrapper<V>(wrapped);
		case In:
			return new InWrapper<V>(wrapped);
		case Out:
			return new OutWrapper<V>(wrapped);
		case Both:
			return new BothWrapper<V>(wrapped);
		default: throw new IllegalArgumentException("Invalid direction: " + dir);
		}
	}
	
	private static final class UndirectedWrapper<V> extends DirWrapper<V> {

		private UndirectedWrapper(V wrapped) { super(wrapped); }

		@Override
		public Direction getDirection() {
			return Direction.Undirected;
		}
	}

	private static final class InWrapper<V> extends DirWrapper<V> {

		private InWrapper(V wrapped) { super(wrapped); }

		@Override
		public Direction getDirection() {
			return Direction.In;
		}
	}
	
	private static final class OutWrapper<V> extends DirWrapper<V> {

		private OutWrapper(V wrapped) { super(wrapped); }

		@Override
		public Direction getDirection() {
			return Direction.Out;
		}
	}
	
	private static final class BothWrapper<V> extends DirWrapper<V> {

		private BothWrapper(V wrapped) { super(wrapped); }

		@Override
		public Direction getDirection() {
			return Direction.Both;
		}
	}
	
	private static final class AllWrapper<V> extends DirWrapper<V> {

		private AllWrapper(V wrapped) { super(wrapped); }

		@Override
		public Direction getDirection() {
			throw new UnsupportedOperationException("Does not have direction!");
		}
		
		@Override
		public boolean hasDirection() {
			return false;
		}
	}
	
}
