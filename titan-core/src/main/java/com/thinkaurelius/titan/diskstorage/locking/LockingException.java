package com.thinkaurelius.titan.diskstorage.locking;

/**
 * This exception signifies a either a technical failure during locking, such as
 * failure in the connection to the underyling storage system, or an attempt to
 * acquire a lock already held by another transaction.
 * <p/>
 * This is merely an indicator interface for explicit locking exceptions such as
 * {@link TemporaryLockingException} or {@link PermanentLockingException}.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public interface LockingException {

}
