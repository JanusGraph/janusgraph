package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import java.util.List;
import java.util.Map;

/**
 * Determines how vertices are placed in individual graph partitions.
 * A particular implementation determines the partition id of a (newly created) vertex. The vertex is
 * then assigned to said partition by Titan.
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
    public int getPartition(InternalElement element);

    /**
     * Bulk assignment of idAuthorities to vertices.
     * <p/>
     * It is expected that the passed in map contains the partition assignment after this method
     * returns. Any initial values in the map are meaningless and to be ignored.
     * <p/>
     * This is an optional operation. Check with {@link #supportsBulkPlacement()} first.
     *
     * @param vertices Map containing all vertices and their partition placement.
     */
    public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices);

    /**
     * Whether this placement strategy supports bulk placement.
     * If not, then {@link #getPartitions(java.util.Map)} will throw {@link UnsupportedOperationException}
     *
     * @return
     */
    public boolean supportsBulkPlacement();

    /**
     * If Titan is embedded, this method is used to indicate to the placement strategy which
     * part of the partition id space is hosted locally so that vertex and edge placements can be made accordingly
     * (i.e. preferring to assign partitions in the local ranges so that the data is hosted locally which is often
     * faster).
     * <p/>
     * This method can be called at any time while Titan is running. It is typically called right
     * after construction and when the id space is redistributed.
     * <p/>
     * Depending on the storage backend one or multiple ranges of partition ids may be given. However, this list is never
     * emtpy.
     *
     * @param localPartitionIdRanges List of {@link PartitionIDRange}s correspondinging to the locally hosted partitions
     */
    public void setLocalPartitionBounds(List<PartitionIDRange> localPartitionIdRanges);

    /**
     * Called when there are no more idAuthorities left in the given partition. It is expected that the
     * placement strategy will no longer use said partition in its placement suggestions.
     *
     * @param partitionID Id of the partition that has been exhausted.
     */
    public void exhaustedPartition(int partitionID);

}
