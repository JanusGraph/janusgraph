package com.thinkaurelius.titan.graphdb.entitystatus;

public class InMemoryEntity implements InternalEntity {

	public static final InMemoryEntity instance = new InMemoryEntity();
	
	private InMemoryEntity() {}
	
	@Override
	public void setID(long id) {
		throw new UnsupportedOperationException("Virtual entities don't have an ID!");
	}

	@Override
	public void delete() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getID() {
		throw new UnsupportedOperationException("Virtual entities don't have an ID!");
	}

	@Override
	public boolean hasID() {
		return false;
	}

	@Override
	public boolean isAccessible() {
		throw new UnsupportedOperationException("Method only available in transaction context!");
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public boolean isDeleted() {
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
	public boolean isReferenceNode() {
		return false;
	}

}
