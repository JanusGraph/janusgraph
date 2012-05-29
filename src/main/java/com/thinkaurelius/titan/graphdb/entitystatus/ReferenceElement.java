package com.thinkaurelius.titan.graphdb.entitystatus;

public class ReferenceElement extends LocatedElement {

	public ReferenceElement(long id) {
		super(id);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAccessible() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public boolean isRemoved() {
		return false;
	}

	@Override
	public boolean isLoaded() {
		return false;
	}

	@Override
	public boolean isModified() {
		return false;
	}

	@Override
	public boolean isNew() {
		return false;
	}

	@Override
	public boolean isReferenceVertex() {
		return true;
	}
	

}
