package com.thinkaurelius.titan.diskstorage.locking;

public interface LocalLockMediatorProvider {
	
	/**
	 * return a single locallockmediator for any given namespace.
	 * 
	 * for any fixed namespace {@code n}, the same object must be returned every
	 * time {@code get(n)} is called, no matter what thread calls it or how many
	 * times.
	 * 
	 * also, for any two unequal namespace strings {@code n} and {@code m},
	 * {@code get(n)} must not equal {@code get(m)}. in other words, each
	 * namespace must have a distinct mediator.
	 * 
	 * @see locallockmediator
	 * @author Dan LaRocque <dalaro@hopcount.org>
	 * @param namespace
	 *            arbitrary identifier for a local lock mediator
	 * @return the local lock mediator for {@code namespace}
	 */
	public LocalLockMediator get(String namespace);

}
