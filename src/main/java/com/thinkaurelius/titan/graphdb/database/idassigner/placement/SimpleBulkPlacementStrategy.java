package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.util.datastructures.ArraysUtil;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleBulkPlacementStrategy implements IDPlacementStrategy {

    private static final Logger log =
            LoggerFactory.getLogger(SimpleBulkPlacementStrategy.class);

    public static final String CONCURRENT_PARTITIONS_KEY = "num-partitions";
    public static final int CONCURRENT_PARTITIONS_DEFAULT = 25;
    
    private final Random random = new Random();
    
    private final int concurrentPartitions;
    private int[] currentPartitions;
    private int lowerPartitionID=0;
    private int upperPartitionID=0;
    
    public SimpleBulkPlacementStrategy(int concurrentPartitions) {
        Preconditions.checkArgument(concurrentPartitions>0);
        this.concurrentPartitions=concurrentPartitions;
        currentPartitions = new int[concurrentPartitions];
        for (int i=0;i<currentPartitions.length;i++) updateElement(i);
    }
    
    public SimpleBulkPlacementStrategy(Configuration config) {
        this(config.getInt(CONCURRENT_PARTITIONS_KEY,CONCURRENT_PARTITIONS_DEFAULT));
    }

    private final int nextPartitionID() {
        return currentPartitions[random.nextInt(concurrentPartitions)];
    }
    
    private final void updateElement(int index) {
        Preconditions.checkArgument(index>=0 && index<currentPartitions.length);
        Preconditions.checkArgument(upperPartitionID>0,"SimpleBulkPlacementStrategy has not yet been initialized");
        currentPartitions[index]=random.nextInt(upperPartitionID-lowerPartitionID)+lowerPartitionID;
    }

    @Override
    public long getPartition(InternalTitanVertex vertex) {
        return nextPartitionID();
    }

    @Override
    public void getPartitions(Map<InternalTitanVertex, PartitionAssignment> vertices) {
        int partitionID = nextPartitionID();
        for (Map.Entry<InternalTitanVertex,PartitionAssignment> entry : vertices.entrySet()) {
            entry.setValue(new SimplePartitionAssignment(partitionID));
        }
    }

    @Override
    public boolean supportsBulkPlacement() {
        return true;
    }

    @Override
    public void setLocalPartitionBounds(int lowerID, int upperID) {
        Preconditions.checkArgument(lowerID>=0);
        Preconditions.checkArgument(upperID>lowerID);
        lowerPartitionID = lowerID;
        upperPartitionID = upperID;
        for (int i=0;i<currentPartitions.length;i++) {
            if (currentPartitions[i]<lowerPartitionID || currentPartitions[i]>=upperPartitionID) {
                updateElement(i);
            }
        }
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        int index = ArraysUtil.indexOf(currentPartitions,partitionID);
        if (index<0) {
            log.error("Non-existant partition exhausted {} in {}",partitionID, Arrays.toString(currentPartitions));
        } else {
            updateElement(index);
        }
    }
}
