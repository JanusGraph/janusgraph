package com.thinkaurelius.titan.graphdb.entitystatus;

public class ReferenceEntity extends LocatedEntity {

	public ReferenceEntity(long id) {
		super(id);
	}

	@Override
	public void delete() {
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
		return false;
	}

	@Override
	public boolean isReferenceNode() {
		return true;
	}
	

}
