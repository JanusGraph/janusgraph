package com.thinkaurelius.titan.graphdb.entitystatus;

import static com.thinkaurelius.titan.graphdb.entitystatus.EntityLifeCycle.*;

/**
 * Virtual Entities are never released, that is, they do not keep track of how many times
 * they have been accessed and released. This simplifies treatment; for INSTANCE, we do not have to
 * "unload" EdgeTypes after release.
 * 
 * A VirtualElement does not keep track of the type of edges being loaded which will be inefficient
 * in any actual implementation. Use one of its child classes to get this functionality.
 * 
 * @author matthias
 *
 */
public class VirtualElement implements InternalElement {
	
	private byte lifecycle;
	
	public VirtualElement(boolean isNew) {
		if (isNew)
			lifecycle = New;
		else 
			lifecycle = Loaded;
	}
	
	VirtualElement(VirtualElement clone) {
		lifecycle = clone.lifecycle;
	}

	
	/* ---------------------------------------------------------------
	 * Lifecycle Inquiry
	 * ---------------------------------------------------------------
	 */

	@Override
	public boolean isModified() {
		return lifecycle == Modified;
	}

	@Override
	public boolean isRemoved() {
		return lifecycle == Deleted;
	}


	@Override
	public boolean isLoaded() {
		return lifecycle == Loaded;
	}

	@Override
	public boolean isNew() {
		return lifecycle == New;
	}
	
	@Override
	public boolean isAccessible() {
		throw new UnsupportedOperationException("Method only available in transaction context");
	}


	@Override
	public boolean isAvailable() {
		return lifecycle==New || lifecycle==Modified || lifecycle==Loaded;
	}

	@Override
	public boolean isReferenceVertex() {
		return false;
	}

	/* ---------------------------------------------------------------
	 * Changes and Lifecycle updates
	 * ---------------------------------------------------------------
	 */

	public final boolean modified() {
		switch(lifecycle) {
		case Deleted: throw new IllegalStateException("Element is already deleted");
		case New: return false;
		case Modified: return false;
		case Loaded: lifecycle = Modified; return true;
		default: throw new IllegalStateException("Current lifecycle code: " + lifecycle);
		}
	}
	
	@Override
	public void remove() {
		switch(lifecycle) {
		case Deleted: throw new IllegalStateException("Element is already deleted");
		case New: 
		case Modified: 
		case Loaded: lifecycle = Deleted; break;
		default: throw new IllegalStateException("Current lifecycle code: " + lifecycle);
		}
	}
	
	public final boolean saved() {
		switch(lifecycle) {
		case Deleted: 
			return false;
		case New: lifecycle = Loaded; return true;
		case Modified: lifecycle = Loaded; return true;
		case Loaded: return false;
		default: throw new IllegalStateException("Current lifecycle code: " + lifecycle);
		}
		
	}
	
	public final void resetNew() {
		switch(lifecycle) {
		case Deleted: 
			throw new IllegalStateException();
		case New:
		case Modified:
		case Loaded: lifecycle = New; break;
		default: throw new IllegalStateException("Current lifecycle code: " + lifecycle);
		}
	}

	

	
	/* ---------------------------------------------------------------
	 * ID Management
	 * ---------------------------------------------------------------
	 */

	@Override
	public long getID() {
		throw new IllegalStateException("The entity has not yet been assigned an id");
	}


	@Override
	public boolean hasID() {
		return false;
	}

	
	@Override
	public void setID(long id) {
		throw new UnsupportedOperationException();
	}


	




}
