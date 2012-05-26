package com.thinkaurelius.titan.diskstorage.locking;

public interface LocalLockMediatorProvider {
	
	public LocalLockMediator get(String namespace);

}
