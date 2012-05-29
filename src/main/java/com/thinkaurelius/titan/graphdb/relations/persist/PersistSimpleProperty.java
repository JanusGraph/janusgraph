package com.thinkaurelius.titan.graphdb.relations.persist;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.SimpleProperty;
import com.thinkaurelius.titan.graphdb.entitystatus.BasicElement;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.graphdb.vertices.VertexUtil;

public class PersistSimpleProperty extends SimpleProperty {

	protected BasicElement entity;
	
	public PersistSimpleProperty(TitanKey type, InternalTitanVertex node, Object attribute) {
		super(type, node, attribute);
		entity = new BasicElement();
	}

	public PersistSimpleProperty(TitanKey type, InternalTitanVertex node, Object attribute, long id) {
		super(type, node, attribute);
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
	public boolean isReferenceVertex() {
		return entity.isReferenceVertex();
	}

	


	
}
