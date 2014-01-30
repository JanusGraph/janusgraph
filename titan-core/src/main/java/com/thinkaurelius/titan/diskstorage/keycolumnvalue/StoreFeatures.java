package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;

/**
 * Describes features supported by a storage backend.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque <dalaro@hopcount.org>
 */

public interface StoreFeatures {

    /**
     * Equivalent to calling {@link#supportsUnorderedScan() {@code ||}
     * {@link #isOrderedScannable()}.
     */
    public boolean hasScan();

    /**
     * Whether this storage backend supports global key scans via
     * {@link KeyColumnValueStore#getKeys(SliceQuery, StoreTransaction)}.
     */
    public boolean hasUnorderedScan();

    /**
     * Whether this storage backend supports global key scans via
     * {@link KeyColumnValueStore#getKeys(KeyRangeQuery, StoreTransaction)}.
     */
    public boolean hasOrderedScan();

    /**
     * Whether this storage backend supports query operations on multiple keys
     * via
     * {@link KeyColumnValueStore#getSlice(java.util.List, SliceQuery, StoreTransaction)}
     */
    public boolean hasMultiQuery();

    /**
     * Whether this store supports locking via
     * {@link KeyColumnValueStore#acquireLock(com.thinkaurelius.titan.diskstorage.StaticBuffer, com.thinkaurelius.titan.diskstorage.StaticBuffer, com.thinkaurelius.titan.diskstorage.StaticBuffer, StoreTransaction)}
     *
     */
    public boolean hasLocking();

    /**
     * Whether this storage backend supports batch mutations via
     * {@link KeyColumnValueStoreManager#mutateMany(java.util.Map, StoreTransaction)}.
     *
     */
    public boolean hasBatchMutation();

    /**
     * Whether this storage backend preserves key locality. This affects Titan's
     * use of vertex ID partitioning.
     *
     */
    public boolean isKeyOrdered();

    /**
     * Whether this storage backend writes and reads data from more than one
     * machine.
     */
    public boolean isDistributed();

    /**
     * Whether this storage backend's transactions support isolation.
     */
    public boolean hasTxIsolation();

    /**
     *
     */
    public boolean hasLocalKeyPartition();

    /**
     * Whether this storage backend provides strong consistency within each
     * key/row. This property is weaker than general strong consistency, since
     * reads and writes to different keys need not obey strong consistency.
     * "Key consistency" is shorthand for
     * "strong consistency at the key/row level".
     *
     * @return true if the backend supports key-level strong consistency
     */
    public boolean isKeyConsistent();

    /**
     * Get a transaction configuration that enforces key consistency. This
     * method has undefined behavior when {@link #isKeyConsistent()} is
     * false.
     *
     * @return a key-consistent tx config
     */
    public Configuration getKeyConsistentTxConfig();

    /**
     * Get a transaction configuration that enforces local key consistency.
     * "Local" has flexible meaning depending on the backend implementation. An
     * example is Cassandra's notion of LOCAL_QUORUM, which provides strong
     * consistency among all replicas in the same datacenter as the node
     * handling the request, but not nodes at other datacenters. This method has
     * undefined behavior when {@link #isKeyConsistent()} is false.
     *
     * Backends which don't support the notion of "local" strong consistency may
     * return the same configuration returned by
     * {@link #getKeyConsistencyTxConfig()}.
     *
     * @return a locally (or globally) key-consistent tx config
     */
    public Configuration getLocalKeyConsistentTxConfig();
}
