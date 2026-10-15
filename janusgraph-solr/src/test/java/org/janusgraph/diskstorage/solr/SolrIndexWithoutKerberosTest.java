// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.diskstorage.solr;

import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class SolrIndexWithoutKerberosTest extends SolrIndexTest {

    @Container
    protected static JanusGraphSolrContainer solrContainer = new JanusGraphSolrContainer();

    protected Configuration getSolrTestConfig() {
        return solrContainer.getLocalSolrTestConfig();
    }

    @Test
    public void testCreateCollectionWithMoreShardsThanNodes() throws Exception {
        // The container runs a single Solr node. Solr 8 puts at most max-shards-per-node replicas of a new collection on
        // a node, Solr 9 has no such limit.
        final String index = "solr";
        final String collection = "twoShards";
        final ModifiableConfiguration config = solrContainer.getLocalSolrTestConfig(
            GraphDatabaseConfiguration.buildGraphConfiguration(), index);
        config.set(SolrIndex.NUM_SHARDS, 2, index);
        config.set(SolrIndex.MAX_SHARDS_PER_NODE, 2, index);
        config.set(SolrIndex.SOLR_DEFAULT_CONFIG, "store1", index);
        final SolrIndex twoShardsIndex = new SolrIndex(config.restrictTo(index));
        try {
            final BaseTransaction registerTx = twoShardsIndex.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MILLI));
            twoShardsIndex.register(collection, TEXT, allKeys.get(TEXT), registerTx);
            registerTx.commit();
        } finally {
            twoShardsIndex.close();
        }

        try (CloudSolrClient client = new CloudLegacySolrClient.Builder(
                Arrays.asList(config.get(SolrIndex.ZOOKEEPER_URL, index)), Optional.empty()).build()) {
            client.connect();
            assertEquals(2, ZkStateReader.from(client).getClusterState().getCollection(collection).getSlices().size());
        }
    }
}
