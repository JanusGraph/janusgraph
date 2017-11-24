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

package org.janusgraph.graphdb.database.idassigner.placement;

/**
 * Utility interface used in {@link IDPlacementStrategy} to hold the partition assignment of
 * a vertex (if it is already assigned a partition) or to be assigned a partition id.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface PartitionAssignment {

    /**
     * Default assignment (when no id has been assigned yet)
     */
    PartitionAssignment EMPTY = () -> -1;

    /**
     * Returns the assigned partition id
     * @return
     */
    int getPartitionID();

}
