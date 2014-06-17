package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A id placement strategy that assigns all vertices created in a transaction
 * to the same partition id. The partition id is selected randomly from a set
 * of partition ids that are retrieved upon initialization.
 *
 * The number of partition ids to choose from is configurable.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleBulkPlacementStrategy implements IDPlacementStrategy {

    private static final Logger log =
            LoggerFactory.getLogger(SimpleBulkPlacementStrategy.class);

    public static final ConfigOption<Integer> CONCURRENT_PARTITIONS = new ConfigOption<Integer>(GraphDatabaseConfiguration.IDS_NS,
            "num-partitions","Number of partitions to allocate for placement", ConfigOption.Type.MASKABLE,10);

    public static final int PARTITION_FINDING_ATTEMPTS = 1000;

    private final Random random = new Random();

    private final int[] currentPartitions;
    private List<PartitionIDRange> localPartitionIdRanges;
    private final Set<Integer> exhaustedPartitions;

    public SimpleBulkPlacementStrategy(int concurrentPartitions) {
        Preconditions.checkArgument(concurrentPartitions > 0);
        currentPartitions = new int[concurrentPartitions];
        exhaustedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<Integer,Boolean>());
    }

    public SimpleBulkPlacementStrategy(Configuration config) {
        this(config.get(CONCURRENT_PARTITIONS));
    }

    private final int nextPartitionID() {
        return currentPartitions[random.nextInt(currentPartitions.length)];
    }

    private final void updateElement(int index) {
        Preconditions.checkArgument(localPartitionIdRanges!=null && !localPartitionIdRanges.isEmpty(),"Local partition id ranges have not been initialized");
        int newPartition;
        int attempts = 0;
        do {
            attempts++;
            newPartition = localPartitionIdRanges.get(random.nextInt(localPartitionIdRanges.size())).getRandomID();
            if (attempts>PARTITION_FINDING_ATTEMPTS) throw new IDPoolExhaustedException("Could not find non-exhausted partition");
        } while (exhaustedPartitions.contains(newPartition));
        currentPartitions[index] = newPartition;
        log.debug("Setting partition at index [{}] to: {}",index,newPartition);
    }

    @Override
    public int getPartition(InternalElement element) {
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
    public void setLocalPartitionBounds(List<PartitionIDRange> localPartitionIdRanges) {
        Preconditions.checkArgument(localPartitionIdRanges!=null && !localPartitionIdRanges.isEmpty());
        this.localPartitionIdRanges = Lists.newArrayList(localPartitionIdRanges); //copy
        for (int i = 0; i < currentPartitions.length; i++) {
            updateElement(i);
        }
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        exhaustedPartitions.add(partitionID);
        boolean found = false;
        for (int i = 0; i < currentPartitions.length; i++) {
            if (currentPartitions[i] == partitionID) {
                updateElement(i);
                found = true;
            }
        }
    }
}
