package com.thinkaurelius.titan.graphdb.edges.persist;

import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edges.SimpleBinaryRelationship;
import com.thinkaurelius.titan.graphdb.entitystatus.BasicEntity;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import com.thinkaurelius.titan.graphdb.vertices.NodeUtil;

public class PersistSimpleBinaryRelationship extends SimpleBinaryRelationship {

	protected BasicEntity entity;
	
	public PersistSimpleBinaryRelationship(RelationshipType type,
			InternalNode start, InternalNode end) {
		super(type, start, end);
		entity = new BasicEntity();
	}

	public PersistSimpleBinaryRelationship(RelationshipType type,
			InternalNode start, InternalNode end, long id) {
		super(type, start, end);
		entity = new BasicEntity(id);
	}

	@Override
	public int hashCode() {
		if (hasID()) return NodeUtil.getIDHashCode(this);
		else return super.hashCode();
	}

	@Override
	public boolean equals(Object oth) {
		if (oth==this) return true;
		else if (!(oth instanceof InternalEdge)) return false;
		InternalEdge other = (InternalEdge)oth;
		if (hasID() || other.hasID()) {
			if (hasID() && other.hasID()) return NodeUtil.equalIDs(this, other);
			else return false;
		}
		return super.equals(other);
	}
	
	@Override
	public void forceDelete() {
		super.forceDelete();
		entity.delete();
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
	public boolean isDeleted() {
		return entity.isDeleted();
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
