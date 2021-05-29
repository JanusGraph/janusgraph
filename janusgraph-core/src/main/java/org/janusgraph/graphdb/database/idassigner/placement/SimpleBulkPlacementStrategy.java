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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.database.idassigner.IDPoolExhaustedException;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
@PreInitializeConfigOptions
public class SimpleBulkPlacementStrategy implements IDPlacementStrategy {

    private static final Logger log =
            LoggerFactory.getLogger(SimpleBulkPlacementStrategy.class);

    public static final ConfigOption<Integer> CONCURRENT_PARTITIONS = new ConfigOption<>(
            GraphDatabaseConfiguration.IDS_NS, "num-partitions",
        "Number of partition block to allocate for placement of vertices", ConfigOption.Type.MASKABLE, 10);

    public static final int PARTITION_FINDING_ATTEMPTS = 1000;

    private final Random random = new Random();

    private final int[] currentPartitions;
    private List<PartitionIDRange> localPartitionIdRanges;
    private final Set<Integer> exhaustedPartitions;

    public SimpleBulkPlacementStrategy(int concurrentPartitions) {
        Preconditions.checkArgument(concurrentPartitions > 0);
        currentPartitions = new int[concurrentPartitions];
        exhaustedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public SimpleBulkPlacementStrategy(Configuration config) {
        this(config.get(CONCURRENT_PARTITIONS));
    }

    private int nextPartitionID() {
        return currentPartitions[random.nextInt(currentPartitions.length)];
    }

    private void updateElement(int index) {
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
    public void injectIDManager(IDManager idManager) {} //We don't need the IDManager here

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
        this.localPartitionIdRanges = new ArrayList<>(localPartitionIdRanges); //copy
        for (int i = 0; i < currentPartitions.length; i++) {
            updateElement(i);
        }
    }

    public boolean isExhaustedPartition(int partitionID) {
        return exhaustedPartitions.contains(partitionID);
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        exhaustedPartitions.add(partitionID);
        for (int i = 0; i < currentPartitions.length; i++) {
            if (currentPartitions[i] == partitionID) {
                updateElement(i);
            }
        }
    }
}
