package com.thinkaurelius.titan.diskstorage.locking;


/**
 * Service provider interface for {@link LocalLockMediators}.
 */
public interface LocalLockMediatorProvider {

    /**
     * Returns a the single {@link LocalLockMediator} responsible for the
     * specified {@code namespace}.
     * <p/>
     * For any given {@code namespace}, the same object must be returned every
     * time {@code get(n)} is called, no matter what thread calls it or how many
     * times.
     * <p/>
     * For any two unequal namespace strings {@code n} and {@code m},
     * {@code get(n)} must not equal {@code get(m)}. in other words, each
     * namespace must have a distinct mediator.
     * 
     * @param namespace
     *            the arbitrary identifier for a local lock mediator
     * @return the local lock mediator for {@code namespace}
     * @author Dan LaRocque <dalaro@hopcount.org>
     * @see LocalLockMediator
     */
    public <T> LocalLockMediator<T> get(String namespace);

}
