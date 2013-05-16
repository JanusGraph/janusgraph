/**
 * Provides discretionary, distributed locking with key-column-value granularity for Titan's storage backends.  The package enforces locking at two levels:
 * 
 * <ul>
 * <li>Between threads in the same process via {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediator}.
 * <li>Between processes on the same or different hosts via reads and writes to a {@link com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore} reserved for locking.
 * </ul>
 * 
 * <h2>Using locking</h2>
 * 
 * There are three methods involved in using locking.
 * 
 * <ul>
 * <li>{@link com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore#acquireLock(java.nio.ByteBuffer, java.nio.ByteBuffer, java.nio.ByteBuffer, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction) KeyColumnValueStore's acquireLock()}
 * <li>{@link com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore#mutate(java.nio.ByteBuffer, java.util.List, java.util.List, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction) KeyColumnValueStore's mutate()}
 * <li>{@link com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction#commit() StoreTransaction's commit()} (or {@code rollback()})
 * </ul>
 * 
 * To claim locks, first call {@code acquireLock()} on the keys, columns, and expected values.
 * To claim multiple locks, call {@code acquireLock()} multiple times.
 * {@code acquireLock()} may throw {@link com.thinkaurelius.titan.diskstorage.locking.LockingException}
 * if it detects a failure to acquire the lock.
 * However, in order to permit optimistic locking,
 * {@code acquireLock()} is not required to detect failures and throw {@code LockingException}.
 * 
 * <p>
 * 
 * To guarantee that all {@code acquireLock()} calls succeeded in obtaining their locks, call {@code mutate()}.
 * The first call to {@code mutate()} automatically checks the result of all previous {@code acquireLock()} calls before mutating data.
 * If any of the locks failed, then {code mutate()} will throw a {@code LockingException}.
 * 
 * <p>
 * 
 * After the first call to {@code mutate()}, no further calls to {@code acquireLock()} are permitted on the transaction.
 * Take all locks required by a transaction before mutating data with that transaction.
 * 
 * <p>
 *
 * To release a transaction's locks, call {@code commit()} (or {@code rollback()}) on the transaction.
 * 
 * <h2>Locking internals overview</h2>
 * 
 * <h3>Enforcement within a process</h3>
 * 
 * Lock contention between transactions in a process is arbitrated by the {@code LocalLockMediator} class.  Each {@link com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore} with lockable data has a single {@code LocalLockMediator} responsible for ensuring that at most one transaction in the JVM holds the lock on any {@code (key, column)} pair at a time.
 * 
 * <p>
 * 
 * Acquiring a lock from a {@code LocalLockMediator} implies that no other transactions in the JVM hold that lock.  However, when Titan is running in a cluster, there may be other Titan processes with transactions that {@code LocalLockMediator} can't track.  This is addressed in the next section.
 * 
 * <h3>Enforcement between processes</h3>
 *
 * Titan manages remote contention using a separate {@code KeyColumnValueStore} called the {@code lockStore} containing only locking protocol records (no graph data are stored in the {@code lockStore}).  The details are implementation-dependent, but for Cassandra and HBase, each column family has its own {@code lockStore}.
 * 
 * <p>
 *
 * After Titan successfully acquires a local lock from a {@code LocalLockMediator}, it writes a single value to the {@lockStore}.
 * 
 * <ul>
 * <li>{@lockStore} key: concatenation of (size of the lock claim key), (lock claim key), (lock claim column)</li>
 * <li>{@lockStore} column: concatenation of (# of nanoseconds since UNIX Epoch), (requestor id)</li>
 * <li>{@lockStore} value: expected value supplied by the user in {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore#acquireLock(java.nio.ByteBuffer, java.nio.ByteBuffer, java.nio.ByteBuffer, com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction) acquireLock()}</li>
 * </ul>
 *
 * In the listing above, "(requestor id)" is an arbitrary sequence of bytes uniquely identifying the Cassandra process within the Titan cluster.  The value and length doesn't matter, so long as its unique.
 * This write must complete in less than {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore#getLockWaitMS() getLockWaitMS()}.  If takes longer, then the write is deleted (if possible) and the lock attempt fails.
 * 
 * <p>
 * 
 * The transaction has now staked a lock claim in the globally-visible {@code lockStore}.  After {@code getLockWaitMS()} elapses since writing the claim, the transaction may determine whether or not the lock attempt succeeded by reading and examining the columns on the {@lockStore key}.  If the column representing the transaction's lock claim is the first one in bytewise-order on the row, then its lock timestamp is the earliest, and it holds the lock.  The implementation also has a configurable lock expiration time, {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore#getLockExpireMS()}, which is the maximum lifetime of a lock.  Columns in {@code lockStore} with timestamps older than this timeout are considered invalid and ignored.
 * 
 * <p>
 * 
 * Unlocking involves deleting the lock claim column from the {@code lockStore} and releasing the lock in {@code LocalLockMediator}.
 * 
 */
package com.thinkaurelius.titan.diskstorage.locking.consistentkey;