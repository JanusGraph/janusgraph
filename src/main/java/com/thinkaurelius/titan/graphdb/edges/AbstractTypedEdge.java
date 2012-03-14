package com.thinkaurelius.titan.graphdb.edges;

import com.thinkaurelius.titan.core.Directionality;
import com.thinkaurelius.titan.core.EdgeCategory;
import com.thinkaurelius.titan.core.EdgeType;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.vertices.NewEmptyNode;

public abstract class AbstractTypedEdge extends NewEmptyNode implements InternalEdge {

	protected final InternalEdgeType type;
	
	public AbstractTypedEdge(EdgeType type) {
		assert type!=null;
		assert type instanceof InternalEdgeType;
		this.type=(InternalEdgeType)type;
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
		throw new CloneNotSupportedException("To be implemented!");
	}
	
	@Override
	public void delete() {
		if (!isModifiable()) throw new UnsupportedOperationException("This edge is unmodifiable and hence cannot be deleted!");
		forceDelete();
	}
	
	@Override
	public void forceDelete() {
		getTransaction().deletedEdge(this);
	}
	
	@Override
	public boolean isInline() {
		return false;
	}
	
	/* ---------------------------------------------------------------
	 * EdgeType methods
	 * ---------------------------------------------------------------
	 */

	@Override
	public EdgeType getEdgeType() {
		return type;
	}

	@Override
	public boolean isUndirected() {
		return type.getDirectionality()==Directionality.Undirected;
	}
	
	@Override
	public boolean isDirected() {
		return type.getDirectionality()==Directionality.Directed;
	}
	
	@Override
	public boolean isUnidirected() {
		return type.getDirectionality()==Directionality.Unidirected;
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
		return type.getCategory()==EdgeCategory.Simple;
	}



	
}
