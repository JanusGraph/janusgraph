package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.tinkerpop.blueprints.Direction;

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
			if (dir.impliedBy(d1) && dir.impliedBy(d2)) return true;
		}
		return false;
	}

	public static final DirWrapper<TitanLabel> wrap(TitanLabel type) {
		return wrapInternal(null,type);
	}
	
	public static final DirWrapper<TitanLabel> wrap(Direction dir, TitanLabel type) {
		return wrapInternal(dir,type);
	}
	
	public static final DirWrapper<TitanType> wrap(TitanType type) {
		return wrapInternal(null,type);
	}
	
	public static final DirWrapper<TitanType> wrap(Direction dir, TitanType type) {
		return wrapInternal(dir,type);
	}
	
	public static final DirWrapper<TypeGroup> wrap(TypeGroup group) {
		return wrapInternal(null,group);
	}

	public static final DirWrapper<TypeGroup> wrap(Direction dir, TypeGroup group) {
		return wrapInternal(dir,group);
	}
	
	private static final<V> DirWrapper<V> wrapInternal(Direction dir, V wrapped) {
		Preconditions.checkNotNull(wrapped);
		if (dir==null) return new AllWrapper<V>(wrapped);
		switch(dir) {
		case IN:
			return new InWrapper<V>(wrapped);
		case OUT:
			return new OutWrapper<V>(wrapped);
		case BOTH:
			return new BothWrapper<V>(wrapped);
		default: throw new IllegalArgumentException("Invalid direction: " + dir);
		}
	}

	private static final class InWrapper<V> extends DirWrapper<V> {

		private InWrapper(V wrapped) { super(wrapped); }

		@Override
		public Direction getDirection() {
			return Direction.IN;
		}
	}
	
	private static final class OutWrapper<V> extends DirWrapper<V> {

		private OutWrapper(V wrapped) { super(wrapped); }

		@Override
		public Direction getDirection() {
			return Direction.OUT;
		}
	}
	
	private static final class BothWrapper<V> extends DirWrapper<V> {

		private BothWrapper(V wrapped) { super(wrapped); }

		@Override
		public Direction getDirection() {
			return Direction.BOTH;
		}
	}
	
	private static final class AllWrapper<V> extends DirWrapper<V> {

		private AllWrapper(V wrapped) { super(wrapped); }

		@Override
		public Direction getDirection() {
			throw new UnsupportedOperationException("Does not have direction");
		}
		
		@Override
		public boolean hasDirection() {
			return false;
		}
	}
	
}
