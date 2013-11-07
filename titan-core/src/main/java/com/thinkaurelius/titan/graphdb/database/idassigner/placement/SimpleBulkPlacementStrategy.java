package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleBulkPlacementStrategy implements IDPlacementStrategy {

    private static final Logger log =
            LoggerFactory.getLogger(SimpleBulkPlacementStrategy.class);

    public static final String CONCURRENT_PARTITIONS_KEY = "num-partitions";
    public static final int CONCURRENT_PARTITIONS_DEFAULT = 10;

    private final Random random = new Random();

    private final int[] currentPartitions;
    private int lowerPartitionID = -1;
    private int partitionWidth = -1;
    private int idCeiling = -1;
//    private IntSet exhaustedPartitions = new IntHashSet(); NOT THREAD SAFE!!

    public SimpleBulkPlacementStrategy(int concurrentPartitions) {
        Preconditions.checkArgument(concurrentPartitions > 0);
        currentPartitions = new int[concurrentPartitions];
    }

    public SimpleBulkPlacementStrategy(Configuration config) {
        this(config.getInt(CONCURRENT_PARTITIONS_KEY, CONCURRENT_PARTITIONS_DEFAULT));
    }

    private final int nextPartitionID() {
        return currentPartitions[random.nextInt(currentPartitions.length)];
    }

    private final void updateElement(int index) {
        Preconditions.checkArgument(lowerPartitionID >= 0 && partitionWidth > 0 && idCeiling > 0);
        Preconditions.checkArgument(index >= 0 && index < currentPartitions.length);
        currentPartitions[index] = (random.nextInt(partitionWidth) + lowerPartitionID) % idCeiling;
    }

    @Override
    public int getPartition(InternalElement vertex) {
        return nextPartitionID();
    }

    @Override
    public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
        int partitionID = nextPartitionID();
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
        Preconditions.checkArgument(idLimit > 0);
        Preconditions.checkArgument(lowerID >= 0 && lowerID < idLimit, lowerID);
        Preconditions.checkArgument(upperID >= 0 && upperID <= idLimit, upperID);
        lowerPartitionID = lowerID;
        idCeiling = idLimit;
        if (lowerID < upperID) partitionWidth = upperID - lowerPartitionID;
        else partitionWidth = (idLimit - lowerID) + upperID;
        Preconditions.checkArgument(partitionWidth > 0, partitionWidth);
        for (int i = 0; i < currentPartitions.length; i++) {
            updateElement(i);
        }
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        boolean found = false;
        for (int i = 0; i < currentPartitions.length; i++) {
            if (currentPartitions[i] == partitionID) {
                updateElement(i);
                found = true;
            }
        }
//        if (found) {
//            exhaustedPartitions.add(partitionID);
//        } else {
//            if (!exhaustedPartitions.contains(partitionID))
//                log.error("Non-existant partition exhausted {} in {}", partitionID, Arrays.toString(currentPartitions));
//        }
    }
}
