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
    REGISTER_INDEX(Collections.singleton(SchemaStatus.INSTALLED), Collections.singleton(SchemaStatus.DISABLED)),

    /**
     * Re-builds the index from the graph
     */
    REINDEX(Arrays.asList(SchemaStatus.REGISTERED, SchemaStatus.ENABLED), Arrays.asList(SchemaStatus.INSTALLED, SchemaStatus.DISABLED)),

    /**
     * Enables the index so that it can be used by the query processing engine. An index must be registered before it
     * can be enabled.
     */
    ENABLE_INDEX(Collections.singleton(SchemaStatus.REGISTERED), Arrays.asList(SchemaStatus.INSTALLED, SchemaStatus.DISABLED)),

    /**
     * Disables the index in the graph so that it is no longer used.
     */
    DISABLE_INDEX(Arrays.asList(SchemaStatus.REGISTERED, SchemaStatus.INSTALLED, SchemaStatus.ENABLED), Collections.emptySet()),

    /**
     * Removes the index from the graph (optional operation)
     */
    REMOVE_INDEX(Collections.singleton(SchemaStatus.DISABLED), Arrays.asList(SchemaStatus.REGISTERED,SchemaStatus.INSTALLED,SchemaStatus.ENABLED));

    private final Set<SchemaStatus> applicableStatuses;
    private final Set<SchemaStatus> failureStatuses;

    SchemaAction(Collection<SchemaStatus> applicableStatuses, Collection<SchemaStatus> failureStatuses){
        this.applicableStatuses = Collections.unmodifiableSet(new HashSet<>(applicableStatuses));
        this.failureStatuses = Collections.unmodifiableSet(new HashSet<>(failureStatuses));
    }

    public Set<SchemaStatus> getApplicableStatus() {
        return applicableStatuses;
    }

    public Set<SchemaStatus> getFailureStatus() {
        return failureStatuses;
    }

    public boolean isApplicableStatus(SchemaStatus status) {
        if (failureStatuses.contains(status))
            throw new IllegalArgumentException(String.format("Update action [%s] cannot be invoked for index with status [%s]",this,status));
        return applicableStatuses.contains(status);
    }
}
