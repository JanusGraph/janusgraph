package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import java.util.Map;

/**
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
    public int getPartition(InternalElement vertex) {
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
    public void setLocalPartitionBounds(int lowerID, int upperID, int idLimit) {
        if (lowerID < upperID) {
            Preconditions.checkArgument(lowerID <= partitionID);
            Preconditions.checkArgument(upperID > partitionID);
        } else {
            Preconditions.checkArgument((lowerID <= partitionID && partitionID < idLimit) ||
                    (upperID > partitionID && partitionID >= 0));
        }
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        throw new IllegalStateException("Cannot use a different partition under this strategy!");
    }
}
