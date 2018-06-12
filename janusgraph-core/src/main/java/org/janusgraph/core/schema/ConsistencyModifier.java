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
 * Used to control JanusGraph's consistency behavior on eventually consistent or other non-transactional backend systems.
 * The consistency behavior can be defined for individual {@link JanusGraphSchemaElement}s which then applies to all instances.
 * <p>
 * Consistency modifiers are installed on schema elements via {@link JanusGraphManagement#setConsistency(JanusGraphSchemaElement, ConsistencyModifier)}
 * and can be read using {@link JanusGraphManagement#getConsistency(JanusGraphSchemaElement)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum ConsistencyModifier {

    /**
     * Uses the default consistency model guaranteed by the enclosing transaction against the configured
     * storage backend.
     * <p>
     * What this means exactly, depends on the configuration of the storage backend as well as the (optional) configuration
     * of the enclosing transaction.
     */
    DEFAULT,

    /**
     * Locks will be explicitly acquired to guarantee consistency if the storage backend supports locks.
     * <p>
     * The exact consistency guarantees depend on the configured lock implementation.
     * <p>
     * Note, that locking may be ignored under certain transaction configurations.
     */
    LOCK,


    /**
     * Causes JanusGraph to delete and add a new edge/property instead of overwriting an existing one, hence avoiding potential
     * concurrent write conflicts. This only applies to multi-edges and list-properties.
     * <p>
     * Note, that this potentially impacts how the data should be read.
     */
    FORK

}
