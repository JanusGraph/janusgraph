package com.thinkaurelius.titan.diskstorage.locking;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

/**
 * Cluster-wide discretionary locking.
 * <p>
 * It is recommended that constructors and factories for implementations of this
 * class accept some kind of identifier distinguishing the processees/threads
 * involved in locking from one another. Each process/thread would then have its
 * own instance.
 */
public interface Locker {
    
    /**
     * Prepare to acquire the lock named by {@code lockID}
     * <p>
     * Returns on success and throws an exception on failure.
     * 
     * @param lockID
     *            The return values of the methods {@link Entry#getColumn()} and
     *            {@link Entry#getValue()} together specify the target lock.
     *            Each distinct pair of return values corresponds to a distinct
     *            lock which can be acquired and released separate from the
     *            rest.
     */
    public void writeLock(KeyColumn lockID, StoreTransaction tx) throws StorageException;
    
    /**
     * Attempt to acquire every lock previously specified by calls to
     * {@link #writeLock(Entry)} since either creation of this {@code Locker}
     * instance or the last {@link #unlockAll()} call.
     * <p>
     * In other words, {@code prepareLock(Entry)} must be called at least once
     * before this method is called in order for this method to have an effect.
     * <p>
     * Returns on success and throws an exception on failure.
     * 
     * @param timeout
     *            The maximum wallclock time to attempt locking in the face of
     *            temporary failures (such as the lock being held by another
     *            process) before giving up and throwing an exception
     * @param timeunits
     *            The units of {@code timeout}
     * @throws PermanentLockingException
     * @throws TemporaryLockingException
     */
    public void checkLocks(StoreTransaction tx) throws StorageException;
    
    /**
     * Attempt to release every lock currently held by this instance.
     * <p>
     * Returns on success and throws an exception on failure.
     * 
     * @param timeout
     *            The maximum wallclock time to attempt locking in the face of
     *            temporary failures (such as the lock being held by another
     *            process) before giving up and throwing an exception
     * @param timeunits
     *            The units of {@code timeout}
     * @throws PermanentLockingException
     * @throws TemporaryLockingException
     */
    public void deleteLocks(StoreTransaction tx) throws StorageException;
}
