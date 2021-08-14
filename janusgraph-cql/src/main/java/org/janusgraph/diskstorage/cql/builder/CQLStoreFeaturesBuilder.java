// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql.builder;

import com.datastax.oss.driver.api.core.CqlSession;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.util.system.NetworkUtil;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METADATA_TOKEN_MAP_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.PARTITIONER_NAME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.USE_EXTERNAL_LOCKING;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_PREFIX;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class CQLStoreFeaturesBuilder {

    public CQLStoreFeaturesWrapper build(final CqlSession session, final Configuration configuration, final String[] hostnames){

        StoreFeatures storeFeatures;
        DistributedStoreManager.Deployment deployment;

        final Configuration global = buildGraphConfiguration()
            .set(READ_CONSISTENCY, CQLStoreManager.CONSISTENCY_QUORUM)
            .set(WRITE_CONSISTENCY, CQLStoreManager.CONSISTENCY_QUORUM)
            .set(METRICS_PREFIX, METRICS_SYSTEM_PREFIX_DEFAULT);

        final Configuration local = buildGraphConfiguration()
            .set(READ_CONSISTENCY, CQLStoreManager.CONSISTENCY_LOCAL_QUORUM)
            .set(WRITE_CONSISTENCY, CQLStoreManager.CONSISTENCY_LOCAL_QUORUM)
            .set(METRICS_PREFIX, METRICS_SYSTEM_PREFIX_DEFAULT);

        final Boolean onlyUseLocalConsistency = configuration.get(ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS);

        final Boolean useExternalLocking = configuration.get(USE_EXTERNAL_LOCKING);

        final StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

        fb.batchMutation(true).distributed(true);
        fb.timestamps(true).cellTTL(true);
        fb.keyConsistent((onlyUseLocalConsistency ? local : global), local);
        fb.locking(useExternalLocking);
        fb.optimisticLocking(true);
        fb.multiQuery(false);

        String partitioner = null;
        if (configuration.has(PARTITIONER_NAME)) {
            partitioner = getShortPartitionerName(configuration.get(PARTITIONER_NAME));
        }
        if (session.getMetadata().getTokenMap().isPresent()) {
            String retrievedPartitioner = getShortPartitionerName(session.getMetadata().getTokenMap().get().getPartitionerName());
            if (partitioner == null) {
                partitioner = retrievedPartitioner;
            } else if (!partitioner.equals(retrievedPartitioner)) {
                throw new IllegalArgumentException(String.format("Provided partitioner (%s) does not match with server (%s)",
                    partitioner, retrievedPartitioner));
            }
        } else if (partitioner == null) {
            throw new IllegalArgumentException(String.format("Partitioner name not provided and cannot retrieve it from " +
                "server, please check %s and %s options", PARTITIONER_NAME.getName(), METADATA_TOKEN_MAP_ENABLED.getName()));
        }
        switch (partitioner) {
            case "DefaultPartitioner": // Amazon managed KeySpace uses com.amazonaws.cassandra.DefaultPartitioner
                fb.timestamps(false).cellTTL(false);
            case "RandomPartitioner":
            case "Murmur3Partitioner": {
                fb.keyOrdered(false).orderedScan(false).unorderedScan(true);
                deployment = DistributedStoreManager.Deployment.REMOTE;
                break;
            }
            case "ByteOrderedPartitioner": {
                fb.keyOrdered(true).orderedScan(true).unorderedScan(false);
                deployment = (hostnames.length == 1)// mark deployment as local only in case we have byte ordered partitioner and local
                    // connection
                    ? (NetworkUtil.isLocalConnection(hostnames[0])) ? DistributedStoreManager.Deployment.LOCAL : DistributedStoreManager.Deployment.REMOTE
                    : DistributedStoreManager.Deployment.REMOTE;
                break;
            }
            default: {
                throw new IllegalArgumentException("Unrecognized partitioner: " + partitioner);
            }
        }
        storeFeatures = fb.build();

        return new CQLStoreFeaturesWrapper(storeFeatures, deployment);
    }

    private String getShortPartitionerName(String partitioner) {
        if (partitioner == null) return null;
        return partitioner.substring(partitioner.lastIndexOf('.') + 1);
    }
}
