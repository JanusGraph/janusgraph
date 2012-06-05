package com.thinkaurelius.titan.graphdb.entitystatus;

import com.google.common.base.Preconditions;

import static com.thinkaurelius.titan.graphdb.entitystatus.LocatedElement.NoID;

public class BasicElement extends VirtualElement {


	
	public BasicElement() {
		super(true);
	}
	
	public BasicElement(long _id) {
		super(false);
		Preconditions.checkArgument(_id>NoID);
		id = _id;
	}

	/* ---------------------------------------------------------------
	 * ID Management
	 * ---------------------------------------------------------------
	 */
	
	private long id;

	@Override
	public long getID() {
		if (!hasID())
			throw new IllegalStateException("The entity has not yet been assigned an id");
		else return id;
	}

	@Override
	public boolean hasID() {
		return id>NoID;
	}

	
	@Override
	public void setID(long id) {
		Preconditions.checkArgument(isNew(), "Illegal lifecycle status for setting id");
		Preconditions.checkArgument(id!=NoID,"Illegal id: " + id);
		if (hasID()) throw new IllegalStateException("The entity has already been assigned an id");
		this.id = id;
	}
	
}
