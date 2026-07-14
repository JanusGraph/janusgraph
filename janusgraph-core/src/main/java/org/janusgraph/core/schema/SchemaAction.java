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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.janusgraph.core.schema.SchemaStatus.DISCARDED;
import static org.janusgraph.core.schema.SchemaStatus.INSTALLED;
import static org.janusgraph.core.schema.SchemaStatus.REGISTERED;
import static org.janusgraph.core.schema.SchemaStatus.ENABLED;
import static org.janusgraph.core.schema.SchemaStatus.DISABLED;
import static org.janusgraph.core.schema.SchemaStatus.WRITE_ONLY_ENABLED;

/**
 * Update actions to be executed through {@link JanusGraphManagement} in {@link JanusGraphManagement#updateIndex(Index, SchemaAction)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum SchemaAction {

    /**
     * Registers the index with all instances in the graph cluster. After an index is installed, it must be registered
     * with all graph instances.
     * A {@link SchemaStatus#DISABLED} index can be re-registered, after which it receives updates for graph
     * mutations again (once the {@link SchemaStatus#REGISTERED} status is reached, all instances write to the index).
     */
    REGISTER_INDEX(Arrays.asList(INSTALLED, DISABLED)),

    /**
     * Re-builds the index from the graph and automatically enables it once the rebuild is finished.
     * If the index has status {@link SchemaStatus#WRITE_ONLY_ENABLED}, its status is preserved instead
     * of automatically enabling the index. Use {@link #ENABLE_INDEX} to enable it for answering queries
     * when needed.
     */
    REINDEX(Arrays.asList(REGISTERED, ENABLED, DISABLED, WRITE_ONLY_ENABLED)),

    /**
     * Removes stale index entries: entries which reference elements that no longer exist in the graph, for
     * example because the element was deleted while an index status change was still propagating through the
     * cluster or while the index was disabled. This action complements {@link #REINDEX}: a reindex restores
     * missing entries for existing elements but never removes entries, while this action removes entries of
     * deleted elements but never adds entries. The index status is not changed by this action.
     * Only global graph indexes (composite and mixed) are supported.
     */
    REMOVE_STALE_ENTRIES(Arrays.asList(REGISTERED, ENABLED, DISABLED, WRITE_ONLY_ENABLED)),

    /**
     * Enables the index for write operations only: the index receives updates for graph mutations and can be
     * reindexed, but it is not used for answering queries. An index must be registered before it can be enabled
     * for writes. Use {@link #ENABLE_INDEX} to additionally enable the index for answering queries.
     * In contrast to an index with status {@link SchemaStatus#REGISTERED}, {@link #REINDEX} does not automatically
     * enable an index with status {@link SchemaStatus#WRITE_ONLY_ENABLED}.
     */
    ENABLE_WRITE_ONLY(Arrays.asList(REGISTERED, ENABLED, WRITE_ONLY_ENABLED)),

    /**
     * Enables the index so that it can be used by the query processing engine. An index must be registered before it
     * can be enabled.
     */
    ENABLE_INDEX(Arrays.asList(REGISTERED, DISABLED, ENABLED, WRITE_ONLY_ENABLED)),

    /**
     * Disables the index in the graph so that it is no longer used.
     * An index can also be disabled directly after its creation (within the same management transaction) so that
     * it never receives updates until it is activated via {@link #REGISTER_INDEX}.
     */
    DISABLE_INDEX(Arrays.asList(ENABLED, DISABLED, REGISTERED, WRITE_ONLY_ENABLED, INSTALLED)),

    /**
     * Deletes indexed data and leaves the index in an empty state.
     */
    DISCARD_INDEX(Arrays.asList(DISABLED, REGISTERED, DISCARDED, WRITE_ONLY_ENABLED)),

    /**
     * Removes the internal index vertex, which completely deletes the index
     */
    DROP_INDEX(Collections.singleton(DISCARDED)),

    /**
     * Registers the index as empty which qualifies it for deletion.
     */
    MARK_DISCARDED(Arrays.asList(DISABLED, REGISTERED, DISCARDED, WRITE_ONLY_ENABLED));

    private final Set<SchemaStatus> applicableStatuses;

    SchemaAction(Collection<SchemaStatus> applicableStatuses) {
        this.applicableStatuses = Collections.unmodifiableSet(new HashSet<>(applicableStatuses));
    }

    public Set<SchemaStatus> getApplicableStatus() {
        return applicableStatuses;
    }

    public boolean isApplicableStatus(SchemaStatus status) {
        if (!applicableStatuses.contains(status))
            throw new IllegalArgumentException(String.format("Update action [%s] cannot be invoked for index with status [%s]",this,status));
        return true;
    }
}
