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

import com.google.common.collect.ImmutableSet;

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
    REGISTER_INDEX,

    /**
     * Re-builds the index from the graph
     */
    REINDEX,

    /**
     * Enables the index so that it can be used by the query processing engine. An index must be registered before it
     * can be enabled.
     */
    ENABLE_INDEX,

    /**
     * Disables the index in the graph so that it is no longer used.
     */
    DISABLE_INDEX,

    /**
     * Removes the index from the graph (optional operation)
     */
    REMOVE_INDEX;

    public Set<SchemaStatus> getApplicableStatus() {
        switch(this) {
            case REGISTER_INDEX: return ImmutableSet.of(SchemaStatus.INSTALLED);
            case REINDEX: return ImmutableSet.of(SchemaStatus.REGISTERED,SchemaStatus.ENABLED);
            case ENABLE_INDEX: return ImmutableSet.of(SchemaStatus.REGISTERED);
            case DISABLE_INDEX: return ImmutableSet.of(SchemaStatus.REGISTERED,SchemaStatus.INSTALLED,SchemaStatus.ENABLED);
            case REMOVE_INDEX: return ImmutableSet.of(SchemaStatus.DISABLED);
            default: throw new IllegalArgumentException("Action is invalid: " + this);
        }
    }

    public Set<SchemaStatus> getFailureStatus() {
        switch(this) {
            case REGISTER_INDEX: return ImmutableSet.of(SchemaStatus.DISABLED);
            case REINDEX: return ImmutableSet.of(SchemaStatus.INSTALLED, SchemaStatus.DISABLED);
            case ENABLE_INDEX: return ImmutableSet.of(SchemaStatus.INSTALLED, SchemaStatus.DISABLED);
            case DISABLE_INDEX: return ImmutableSet.of();
            case REMOVE_INDEX: return ImmutableSet.of(SchemaStatus.REGISTERED,SchemaStatus.INSTALLED,SchemaStatus.ENABLED);
            default: throw new IllegalArgumentException("Action is invalid: " + this);
        }
    }

    public boolean isApplicableStatus(SchemaStatus status) {
        if (getFailureStatus().contains(status))
            throw new IllegalArgumentException(String.format("Update action [%s] cannot be invoked for index with status [%s]",this,status));
        return getApplicableStatus().contains(status);
    }

}
