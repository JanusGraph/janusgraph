package com.thinkaurelius.titan.graphdb.entitystatus;

import com.google.common.base.Preconditions;


public abstract class LocatedElement implements InternalElement {

	static final long NoID = 0;
	
	private long id;
	
	public LocatedElement() {
		id = NoID;
	}
	
	public LocatedElement(long _id) {
		Preconditions.checkArgument(_id>NoID);
		id = _id;
	}
	
	LocatedElement(LocatedElement clone) {
		id = clone.id;
	}
	
	/* ---------------------------------------------------------------
	 * ID Management
	 * ---------------------------------------------------------------
	 */

	@Override
	public long getID() {
		if (!hasID()) throw new IllegalStateException("The entity has not yet been assigned an id");
		else return id;
	}

	@Override
	public boolean hasID() {
		return id>NoID;
	}

	
	@Override
	public void setID(long id) {
		Preconditions.checkArgument(isNew());
		Preconditions.checkArgument(id!=NoID,"Illegal id: " + id);
		if (hasID()) throw new IllegalStateException("The entity has already been assigned an id: " + getID());
		this.id = id;
	}
	
	
}
