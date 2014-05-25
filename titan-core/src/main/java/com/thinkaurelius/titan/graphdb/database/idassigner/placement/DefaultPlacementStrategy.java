package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import java.util.List;
import java.util.Map;

/**
 * Default {@link IDPlacementStrategy} that assigns all vertices to a fixed partition configured upon construction.
 * This strategy is used when partitioning is disabled. This strategy is not configurable.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class DefaultPlacementStrategy implements IDPlacementStrategy {

    private final int partitionID;

    public DefaultPlacementStrategy(int partitionID) {
        Preconditions.checkArgument(partitionID >= 0);
        this.partitionID = partitionID;
    }

    public DefaultPlacementStrategy() {
        this(0);
    }

    @Override
    public int getPartition(InternalElement element) {
        return partitionID;
    }

    @Override
    public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
        for (Map.Entry<InternalVertex, PartitionAssignment> entry : vertices.entrySet()) {
            entry.setValue(new SimplePartitionAssignment(partitionID));
        }
    }

    @Override
    public boolean supportsBulkPlacement() {
        return true;
    }

    @Override
    public void setLocalPartitionBounds(List<PartitionIDRange> localPartitionIdRanges) {
        boolean isContained = false;
        for (PartitionIDRange range : localPartitionIdRanges) {
            if (range.contains(partitionID)) {
                isContained = true;
                break;
            }
        }
        Preconditions.checkArgument(isContained,"None of the local partition id ranges contains the configured partition id: %s",partitionID);
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        throw new IllegalStateException("Cannot use a different partition under this strategy!");
    }
}
