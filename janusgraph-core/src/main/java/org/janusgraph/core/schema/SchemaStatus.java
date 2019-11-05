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
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum SchemaStatus {

    /**
     * The index is installed in the system but not yet registered with all instances in the cluster
     */
    INSTALLED,

    /**
     * The index is registered with all instances in the cluster but not (yet) enabled
     */
    REGISTERED,

    /**
     * The index is enabled and in use
     */
    ENABLED,

    /**
     * The index is disabled and no longer in use
     */
    DISABLED;


    public boolean isStable() {
        return this != SchemaStatus.INSTALLED;
    }
}
