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

import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalVertex;

import java.util.List;
import java.util.Map;

/**
 * Determines how vertices are placed in individual graph partitions.
 * A particular implementation determines the partition id of a (newly created) vertex. The vertex is
 * then assigned to said partition by JanusGraph.
 *
 * The id placement strategy is configurable.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IDPlacementStrategy {

    /**
     * Individually assigns an id to the given vertex or relation.
     *
     * @param element Vertex or relation to assign id to.
     * @return
     */
    int getPartition(InternalElement element);

    /**
     * Bulk assignment of idAuthorities to vertices.
     * <p>
     * It is expected that the passed in map contains the partition assignment after this method
     * returns. Any initial values in the map are meaningless and to be ignored.
     * <p>
     * This is an optional operation. Check with {@link #supportsBulkPlacement()} first.
     *
     * @param vertices Map containing all vertices and their partition placement.
     */
    void getPartitions(Map<InternalVertex, PartitionAssignment> vertices);

    /**
     * After construction, the {@link org.janusgraph.graphdb.idmanagement.IDManager} used by this graph instance
     * is injected into this IDPlacementStrategy so that the id pattern of vertices can be inspected.
     * This method is guaranteed to be called before any partition assignments are made.
     *
     * @param idManager
     */
    void injectIDManager(IDManager idManager);

    /**
     * Whether this placement strategy supports bulk placement.
     * If not, then {@link #getPartitions(java.util.Map)} will throw {@link UnsupportedOperationException}
     *
     * @return
     */
    boolean supportsBulkPlacement();

    /**
     * If JanusGraph is embedded, this method is used to indicate to the placement strategy which
     * part of the partition id space is hosted locally so that vertex and edge placements can be made accordingly
     * (i.e. preferring to assign partitions in the local ranges so that the data is hosted locally which is often
     * faster).
     * <p>
     * This method can be called at any time while JanusGraph is running. It is typically called right
     * after construction and when the id space is redistributed.
     * <p>
     * Depending on the storage backend one or multiple ranges of partition ids may be given. However, this list is never
     * empty.
     *
     * @param localPartitionIdRanges List of {@link PartitionIDRange}s corresponding to the locally hosted partitions
     */
    void setLocalPartitionBounds(List<PartitionIDRange> localPartitionIdRanges);

    /**
     * Called when there are no more idAuthorities left in the given partition. It is expected that the
     * placement strategy will no longer use said partition in its placement suggestions.
     *
     * @param partitionID Id of the partition that has been exhausted.
     */
    void exhaustedPartition(int partitionID);

}
