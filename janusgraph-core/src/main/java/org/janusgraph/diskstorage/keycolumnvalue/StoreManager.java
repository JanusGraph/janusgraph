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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;

import java.util.List;

/**
 * Generic interface to a backend storage engine.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface StoreManager {

    /**
     * Returns a transaction handle for a new transaction according to the given configuration.
     *
     * @return New Transaction Handle
     */
    StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException;

    /**
     * Closes the Storage Manager and all databases that have been opened.
     */
    void close() throws BackendException;


    /**
     * Deletes and clears all database in this storage manager.
     * <p>
     * ATTENTION: Invoking this method will delete ALL your data!!
     */
    void clearStorage() throws BackendException;

    /**
     * Check whether database exists in this storage manager.
     * @return Flag indicating whether database exists
     * @throws BackendException
     */
    boolean exists() throws BackendException;

    /**
     * Returns the features supported by this storage manager
     *
     * @return The supported features of this storage manager
     * @see StoreFeatures
     */
    StoreFeatures getFeatures();

    /**
     * Return an identifier for the StoreManager. Two managers with the same
     * name would open databases that read and write the same underlying data;
     * two store managers with different names should be, for data read/write
     * purposes, completely isolated from each other.
     * <p>
     * Examples:
     * <ul>
     * <li>Cassandra keyspace</li>
     * <li>HBase tablename</li>
     * <li>InMemoryStore heap address (i.e. default toString()).</li>
     * </ul>
     *
     * @return Name for this StoreManager
     */
    String getName();

    /**
     * Returns {@code KeyRange}s locally hosted on this machine. The start of
     * each {@code KeyRange} is inclusive. The end is exclusive. The start and
     * end must each be at least 4 bytes in length.
     *
     * @return A list of local key ranges
     * @throws UnsupportedOperationException
     *             if the underlying store does not support this operation.
     *             Check {@link StoreFeatures#hasLocalKeyPartition()} first.
     */
    List<KeyRange> getLocalKeyPartition() throws BackendException;

    /**
     * Returns {@code org.janusgraph.hadoop.HadoopStoreManager}
     *
     * @return A {@code HadoopStoreManager} if supported.
     */
    default Object getHadoopManager() throws BackendException {
        throw new UnsupportedOperationException("This Manager doesn't support hadoop");
    }
}
