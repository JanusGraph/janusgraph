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

/**
 * Update actions to be executed through {@link JanusGraphManagement} in {@link JanusGraphManagement#updateIndex(Index, SchemaAction)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum SchemaAction {

    /**
     * Registers the index with all instances in the graph cluster. After an index is installed, it must be registered
     * with all graph instances.
     */
    REGISTER_INDEX(Collections.singleton(INSTALLED)),

    /**
     * Re-builds the index from the graph
     */
    REINDEX(Arrays.asList(REGISTERED, ENABLED, DISABLED)),

    /**
     * Enables the index so that it can be used by the query processing engine. An index must be registered before it
     * can be enabled.
     */
    ENABLE_INDEX(Arrays.asList(REGISTERED, DISABLED, ENABLED)),

    /**
     * Disables the index in the graph so that it is no longer used.
     */
    DISABLE_INDEX(Arrays.asList(ENABLED, DISABLED, REGISTERED)),

    /**
     * Deletes indexed data and leaves the index in an empty state.
     */
    DISCARD_INDEX(Arrays.asList(DISABLED, REGISTERED, DISCARDED)),

    /**
     * Removes the internal index vertex, which completely deletes the index
     */
    DROP_INDEX(Collections.singleton(DISCARDED)),

    /**
     * Registers the index as empty which qualifies it for deletion.
     */
    MARK_DISCARDED(Arrays.asList(DISABLED, REGISTERED, DISCARDED));

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
