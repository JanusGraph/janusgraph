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

package org.janusgraph.core.schema;

/**
 * Designates the status of a {@link Index} in a graph.
 * <p>
 * Note: the status is persisted by its ordinal (see
 * {@code org.janusgraph.graphdb.database.serialize.attribute.EnumSerializer}), so new constants
 * must only ever be appended at the end and existing constants must never be reordered or removed.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum SchemaStatus {

    /**
     * The index is installed in the system but not yet registered with all instances in the cluster.
     * The index already receives updates for graph mutations on the instances that know about it,
     * but it is not used for answering queries.
     */
    INSTALLED,

    /**
     * The index is registered with all instances in the cluster but not (yet) enabled for answering queries.
     * The index receives updates for all graph mutations, which makes it safe to reindex existing data,
     * but it is not used for answering queries.
     */
    REGISTERED,

    /**
     * The index is enabled and in use: it receives updates for all graph mutations and is used for
     * answering queries.
     */
    ENABLED,

    /**
     * The index is temporarily disabled and not in use: it neither receives updates nor is it used for
     * answering queries.
     */
    DISABLED,

    /**
     * The index is cleared and ready for deletion
     */
    DISCARDED,

    /**
     * The index is enabled for write operations only: it receives updates for all graph mutations,
     * but it is not used for answering queries. In contrast to {@link #REGISTERED}, this status expresses
     * the explicit intent to keep the index write-only, so {@link SchemaAction#REINDEX} preserves this
     * status instead of automatically enabling the index. Use {@link SchemaAction#ENABLE_INDEX} to enable
     * the index for answering queries once needed.
     */
    WRITE_ONLY_ENABLED;

    public boolean isStable() {
        return this != SchemaStatus.INSTALLED;
    }
}
