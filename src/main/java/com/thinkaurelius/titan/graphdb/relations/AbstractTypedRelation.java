package com.thinkaurelius.titan.graphdb.relations;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.vertices.NewEmptyTitanVertex;

public abstract class AbstractTypedRelation extends NewEmptyTitanVertex implements InternalRelation {

	protected final InternalTitanType type;
	
	public AbstractTypedRelation(TitanType type) {
		assert type!=null;
		assert type instanceof InternalTitanType;
		this.type=(InternalTitanType)type;
	}
	
	
	/* ---------------------------------------------------------------
	 * In memory handling
	 * ---------------------------------------------------------------
	 */
	
	@Override
	public int hashCode() {
		throw new UnsupportedOperationException("Needs to be overwritten");
	}
	
	protected final int objectHashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object oth) {
		throw new UnsupportedOperationException("Needs to be overwritten");
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException("To be implemented");
	}
	
	@Override
	public void remove() {
		if (!isModifiable()) throw new UnsupportedOperationException("This edge is unmodifiable and hence cannot be deleted");
		forceDelete();
	}
	
	@Override
	public void forceDelete() {
		getTransaction().deletedRelation(this);
	}
	
	@Override
	public boolean isInline() {
		return false;
	}
	
	/* ---------------------------------------------------------------
	 * TitanType methods
	 * ---------------------------------------------------------------
	 */

	@Override
	public TitanType getType() {
		return type;
	}

	@Override
	public boolean isUndirected() {
		return !type.isPropertyKey() && ((TitanLabel)type).isUndirected();
	}

	@Override
	public boolean isDirected() {
		return type.isPropertyKey() || ((TitanLabel)type).isDirected();
	}

	@Override
	public boolean isUnidirected() {
		return !type.isPropertyKey() && ((TitanLabel)type).isUnidirected();
	}

	@Override
	public boolean isHidden() {
		return type.isHidden();
	}
	
	@Override
	public boolean isModifiable() {
		return type.isModifiable();
	}

	@Override
	public boolean isSimple() {
		return type.isSimple();
	}



	
}
