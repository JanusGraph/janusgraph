package com.thinkaurelius.titan.graphdb.entitystatus;

public class InMemoryElement implements InternalElement {

	public static final InMemoryElement instance = new InMemoryElement();
	
	private InMemoryElement() {}
	
	@Override
	public void setID(long id) {
		throw new UnsupportedOperationException("Virtual entities don't have an id");
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getID() {
		throw new UnsupportedOperationException("Virtual entities don't have an id");
	}

	@Override
	public boolean hasID() {
		return false;
	}

	@Override
	public boolean isAccessible() {
		throw new UnsupportedOperationException("Method only available in transaction context");
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
		return true;
	}

	@Override
	public boolean isReferenceVertex() {
		return false;
	}

}
