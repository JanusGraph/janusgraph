// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.keycolumnvalue;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.util.time.TimestampProviders;

/**
 * Describes features supported by a storage backend.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Dan LaRocque (dalaro@hopcount.org)
 */

public interface StoreFeatures {

    /**
     * Equivalent to calling {@link #hasUnorderedScan()} {@code ||}
     * {@link #hasOrderedScan()}.
     */
    boolean hasScan();

    /**
     * Whether this storage backend supports global key scans via
     * {@link KeyColumnValueStore#getKeys(SliceQuery, StoreTransaction)}.
     */
    boolean hasUnorderedScan();

    /**
     * Whether this storage backend supports a consistent key order among different scans.
     * If it supports ordered scan, it must support consistent key scan.
     * If it doesn't support ordered scan, it may or may not support consistent key scan.
     * If the consistent scan is not supported, the backend should support {@link KeyColumnValueStore#getKeys(MultiSlicesQuery, StoreTransaction)}
     */
    boolean hasConsistentScan();

    /**
     * Whether this storage backend supports global key scans via
     * {@link KeyColumnValueStore#getKeys(KeyRangeQuery, StoreTransaction)}.
     */
    boolean hasOrderedScan();

    /**
     * Whether this storage backend supports query operations on multiple keys
     * via
     * {@link KeyColumnValueStore#getSlice(java.util.List, SliceQuery, StoreTransaction)}
     */
    boolean hasMultiQuery();

    /**
     * Whether this store supports locking via
     * {@link KeyColumnValueStore#acquireLock(org.janusgraph.diskstorage.StaticBuffer, org.janusgraph.diskstorage.StaticBuffer, org.janusgraph.diskstorage.StaticBuffer, StoreTransaction)}
     *
     */
    boolean hasLocking();

    /**
     * Whether this storage backend supports batch mutations via
     * {@link KeyColumnValueStoreManager#mutateMany(java.util.Map, StoreTransaction)}.
     *
     */
    boolean hasBatchMutation();

    /**
     * Whether this storage backend preserves key locality. This affects JanusGraph's
     * use of vertex ID partitioning.
     *
     */
    boolean isKeyOrdered();

    /**
     * Whether this storage backend writes and reads data from more than one
     * machine.
     */
    boolean isDistributed();

    /**
     * Whether this storage backend's transactions support isolation.
     */
    boolean hasTxIsolation();

    /**
     * Whether this storage backend has a (possibly improper) subset of the
     * its accessible data stored locally, that is, partially available for
     * I/O operations without necessarily going over the network.
     *
     * If this is true, then {@link StoreManager#getLocalKeyPartition()} must
     * return a valid list as described in that method.  If this is false, that
     * method will not be invoked.
     */
    boolean hasLocalKeyPartition();

    /**
     * Whether this storage backend provides strong consistency within each
     * key/row. This property is weaker than general strong consistency, since
     * reads and writes to different keys need not obey strong consistency.
     * "Key consistency" is shorthand for
     * "strong consistency at the key/row level".
     *
     * @return true if the backend supports key-level strong consistency
     */
    boolean isKeyConsistent();

    /**
     * Returns true if column-value entries in this storage backend are annotated with a timestamp,
     * else false. It is assumed that the timestamp matches the one set during the committing transaction.
     *
     * @return
     */
    boolean hasTimestamps();

    /**
     * If this storage backend supports one particular type of data
     * timestamp/version better than others. For example, HBase server-side TTLs
     * assume row timestamps are in milliseconds; some Cassandra client utils
     * assume timestamps in microseconds. This method should return null if the
     * backend has no preference for a specific timestamp resolution.
     *
     * This method will be ignored by JanusGraph if {@link #hasTimestamps()} is
     * false.
     *
     * @return null or a Timestamps enum value
     */
    TimestampProviders getPreferredTimestamps();

    /**
     * Returns true if this storage backend support time-to-live (TTL) settings for column-value entries. If such a value
     * is provided as a meta-data annotation on the {@link org.janusgraph.diskstorage.Entry}, the entry will
     * disappear from the storage backend after the given amount of time. See references to
     * {@link org.janusgraph.diskstorage.EntryMetaData#TTL} for example usage in JanusGraph internals.
     * This is the finer-grained of the two TTL modes.
     *
     * @return true if the storage backend supports cell-level TTL, else false
     */
    boolean hasCellTTL();

    /**
     * Returns true if this storage backend supports time-to-live (TTL) settings on a per-store basis. That means, that
     * entries added to such a store will require after a configured amount of time.  Per-store TTL is represented
     * by {@link org.janusgraph.diskstorage.StoreMetaData#TTL}.  This is the coarser-grained of the two
     * TTL modes.
     *
     * @return true if the storage backend supports store-level TTL, else false
     */
    boolean hasStoreTTL();

    /**
     * Returns true if this storage backend supports entry-level visibility by attaching a visibility or authentication
     * token to each column-value entry in the data store and limited retrievals to "visible" entries.
     *
     * @return
     */
    boolean hasVisibility();

    /**
     * Whether the backend supports data persistence. Return false if the backend is in-memory only.
     * @return
     */
    boolean supportsPersistence();

    /**
     * Get a transaction configuration that enforces key consistency. This
     * method has undefined behavior when {@link #isKeyConsistent()} is
     * false.
     *
     * @return a key-consistent tx config
     */
    Configuration getKeyConsistentTxConfig();

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
     * {@link #getKeyConsistentTxConfig()}.
     *
     * @return a locally (or globally) key-consistent tx config
     */
    Configuration getLocalKeyConsistentTxConfig();


    /**
     * Get a transaction configuration suitable for reading data into a
     * {@link ScanJob}.  Transactions opened on this config will only be
     * used to read data, not to write it, and they'll be rolled back
     * when those reads are completed.  The configuration returned by this
     * method should disable transaction isolation, if the store supports it.
     *
     * @return a transaction configuration suitable for scanjob data reading
     */
    Configuration getScanTxConfig();


    /**
     * Whether calls to this manager and its stores may be safely interrupted
     * without leaving the underlying system in an inconsistent state.
     */
    boolean supportsInterruption();

    /**
     * Whether the store will commit pending mutations optimistically and make other pending changes
     * to the same cells fail on tx.commit() (true) or will fail pending mutations pessimistically on tx.commit()
     * if other parallel transactions have already marked the relevant cells dirty.
     * @return
     */
    boolean hasOptimisticLocking();

}
