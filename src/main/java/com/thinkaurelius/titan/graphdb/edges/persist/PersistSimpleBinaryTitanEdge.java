package com.thinkaurelius.titan.graphdb.edges.persist;

import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.edges.InternalRelation;
import com.thinkaurelius.titan.graphdb.edges.SimpleBinaryTitanEdge;
import com.thinkaurelius.titan.graphdb.entitystatus.BasicElement;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.graphdb.vertices.VertexUtil;

public class PersistSimpleBinaryTitanEdge extends SimpleBinaryTitanEdge {

	protected BasicElement entity;
	
	public PersistSimpleBinaryTitanEdge(TitanLabel type,
                                        InternalTitanVertex start, InternalTitanVertex end) {
		super(type, start, end);
		entity = new BasicElement();
	}

	public PersistSimpleBinaryTitanEdge(TitanLabel type,
                                        InternalTitanVertex start, InternalTitanVertex end, long id) {
		super(type, start, end);
		entity = new BasicElement(id);
	}

	@Override
	public int hashCode() {
		if (hasID()) return VertexUtil.getIDHashCode(this);
		else return super.hashCode();
	}

	@Override
	public boolean equals(Object oth) {
		if (oth==this) return true;
		else if (!(oth instanceof InternalRelation)) return false;
		InternalRelation other = (InternalRelation)oth;
		if (hasID() || other.hasID()) {
			if (hasID() && other.hasID()) return VertexUtil.equalIDs(this, other);
			else return false;
		}
		return super.equals(other);
	}
	
	@Override
	public void forceDelete() {
		super.forceDelete();
		entity.remove();
	}

	
	/* ---------------------------------------------------------------
	 * ID Management
	 * ---------------------------------------------------------------
	 */

	@Override
	public long getID() {
		return entity.getID();
	}



	@Override
	public boolean hasID() {
		return entity.hasID();
	}
	
	
	@Override
	public void setID(long id) {
		entity.setID(id);
	}
	

	/* ---------------------------------------------------------------
	 * LifeCycle Management
	 * ---------------------------------------------------------------
	 */

	@Override
	public boolean isModified() {
		return entity.isModified();
	}


	@Override
	public boolean isAvailable() {
		return entity.isAvailable();
	}


	@Override
	public boolean isRemoved() {
		return entity.isRemoved();
	}

	@Override
	public boolean isLoaded() {
		return entity.isLoaded();
	}


	@Override
	public boolean isNew() {
		return entity.isNew();
	}

	@Override
	public boolean isReferenceNode() {
		return entity.isReferenceNode();
	}


}
