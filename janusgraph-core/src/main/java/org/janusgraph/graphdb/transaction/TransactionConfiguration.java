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

package org.janusgraph.graphdb.transaction;

import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.diskstorage.BaseTransactionConfig;

/**
 * Provides configuration options for {@link org.janusgraph.core.JanusGraphTransaction}.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see org.janusgraph.core.JanusGraphTransaction
 */
public interface TransactionConfiguration extends BaseTransactionConfig {

    /**
     * Checks whether the graph transaction is configured as read-only.
     *
     * @return True, if the transaction is configured as read-only, else false.
     */
    boolean isReadOnly();

    /**
     * @return Whether this transaction is configured to assign idAuthorities immediately.
     */
    boolean hasAssignIDsImmediately();


    /**
     * Whether the incident relation data on vertices is being externally pre-loaded.
     * This causes the transaction to only return stub vertices and leave any data loading
     * up to the caller.
     * @return
     */
    boolean hasPreloadedData();


    /**
     * Whether this transaction should be optimized for batch-loading, i.e. ingestion of lots of data.
     *
     * @return
     */
    boolean hasEnabledBatchLoading();

    /**
     * Whether the graph transaction is configured to verify that a vertex with the id GIVEN BY THE USER actually exists
     * in the database or not.
     * In other words, it is verified that user provided vertex ids (through public APIs) actually exist.
     *
     * @return True, if vertex existence is verified, else false
     */
    boolean hasVerifyExternalVertexExistence();

    /**
     * Whether the graph transaction is configured to verify that a vertex with the id actually exists
     * in the database or not on every retrieval.
     * In other words, it is always verified that a vertex for a given id exists, even if that id is retrieved internally
     * (through private APIs).
     * <p>
     * Hence, this is a defensive setting against data degradation, where edges and/or index entries might point to no
     * longer existing vertices. Use this setting with caution as it introduces additional overhead entailed by checking
     * the existence.
     * <p>
     * Unlike {@link #hasVerifyExternalVertexExistence()} this is about internally verifying ids.
     *
     * @return True, if vertex existence is verified, else false
     */
    boolean hasVerifyInternalVertexExistence();

    /**
     * Whether the persistence layer should acquire locks for this transaction during persistence.
     *
     * @return True, if locks should be acquired, else false
     */
    boolean hasAcquireLocks();

    /**
     * @return The default edge type maker used to automatically create not yet existing edge types.
     */
    DefaultSchemaMaker getAutoSchemaMaker();

    /**
     * Allows to disable schema constraints.
     *
     * @return True, if schema constraints should not be applied, else false.
     */
    boolean hasDisabledSchemaConstraints();

    /**
     * Whether the graph transaction is configured to verify that an added key does not yet exist in the database.
     *
     * @return True, if vertex existence is verified, else false
     */
    boolean hasVerifyUniqueness();

    /**
     * Whether this transaction loads all properties on a vertex when a single property is requested. This can be highly beneficial
     * when additional properties are requested on the same vertex at a later time. For vertices with very many properties
     * this might increase latencies of property fetching.
     *
     * @return True, if this transaction pre-fetches all properties
     */
    boolean hasPropertyPrefetching();

    /**
     * Whether this transaction should batch backend queries. This can lead to significant performance improvement
     * if there is non-trivial latency to the backend.
     *
     * @return True, if this transaction has multi-query enabled
     */
    boolean useMultiQuery();

    /**
     * Whether this transaction is only accessed by a single thread.
     * If so, then certain data structures may be optimized for single threaded access since locking can be avoided.
     *
     * @return
     */
    boolean isSingleThreaded();

    /**
     * Whether this transaction is bound to a running thread.
     * If so, then elements in this transaction can expand their life cycle to the next transaction in the thread.
     *
     * @return
     */
    boolean isThreadBound();

    /**
     * The maximum number of recently-used vertices to cache in this transaction.
     * The recently-used vertex cache can include both clean and dirty vertices.
     *
     * @return
     */
    int getVertexCacheSize();

    /**
     * The initial size of the dirty (modified) vertex map used by a transaction.
     *
     * @return
     */
    int getDirtyVertexSize();

    /**
     * The maximum weight for the index cache store used in this particular transaction
     *
     * @return
     */
    long getIndexCacheWeight();

    /**
     * The name of the log to be used for logging the mutations in this transaction.
     * If the identifier is NULL the mutations will not be logged.
     *
     * @return
     */
    String getLogIdentifier();


    /**
     * Whether this transaction should throw an exception when a graph query is issued that cannot be answered
     * with any existing index but instead requires a full graph-scan.
     * @return
     */
    boolean hasForceIndexUsage();


    /**
     * Querying of partitioned vertices is restricted to the partitions returned by this
     * method. If the return value has length 0 all partitions are queried (i.e. unrestricted).
     *
     * @return
     */
    int[] getRestrictedPartitions();

    /**
     * Returns true if the queried partitions should be restricted in this transaction
     */
    boolean hasRestrictedPartitions();

}
