// Copyright 2026 JanusGraph Authors
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

import org.apache.solr.common.params.SolrParams;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Only Solr 8 supports the maxShardsPerNode parameter of the collection creation.
 */
public class SolrIndexCreateCollectionRequestTest {

    private static final String INDEX = "solr";
    private static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";
    private static final Supplier<OptionalInt> NOT_ASKED = () -> {
        throw new AssertionError("Solr mustn't be asked for its version");
    };

    private static Configuration config(Integer solrMajorVersion, Integer maxShardsPerNode) {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(SolrIndex.NUM_SHARDS, 2, INDEX);
        config.set(SolrIndex.SOLR_DEFAULT_CONFIG, "janusgraph-configset", INDEX);
        if (solrMajorVersion != null) {
            config.set(SolrIndex.SOLR_MAJOR_VERSION, solrMajorVersion, INDEX);
        }
        if (maxShardsPerNode != null) {
            config.set(SolrIndex.MAX_SHARDS_PER_NODE, maxShardsPerNode, INDEX);
        }
        return config.restrictTo(INDEX);
    }

    private static SolrParams params(Configuration config, Supplier<OptionalInt> reportedSolrMajorVersion) {
        return SolrIndex.createCollectionRequest(config, "collection", reportedSolrMajorVersion).getParams();
    }

    @Test
    public void testSolr8GetsMaxShardsPerNode() {
        assertEquals("2", params(config(null, 2), () -> OptionalInt.of(8)).get(MAX_SHARDS_PER_NODE));
        // the default of the option is Solr 8's default
        assertEquals("1", params(config(null, null), () -> OptionalInt.of(8)).get(MAX_SHARDS_PER_NODE));
    }

    @Test
    public void testSolr9DoesNotGetMaxShardsPerNode() {
        assertNull(params(config(null, 2), () -> OptionalInt.of(9)).get(MAX_SHARDS_PER_NODE));
        assertNull(params(config(null, null), () -> OptionalInt.of(10)).get(MAX_SHARDS_PER_NODE));
    }

    @Test
    public void testUnknownSolrVersionGetsMaxShardsPerNode() {
        assertEquals("2", params(config(null, 2), OptionalInt::empty).get(MAX_SHARDS_PER_NODE));
    }

    @Test
    public void testConfiguredSolrVersionIsPreferred() {
        assertNull(params(config(9, 2), NOT_ASKED).get(MAX_SHARDS_PER_NODE));
        assertEquals("2", params(config(8, 2), NOT_ASKED).get(MAX_SHARDS_PER_NODE));
    }

    @Test
    public void testRequestsOnlyDifferInMaxShardsPerNode() {
        final Map<String, List<String>> solr8 = toMap(params(config(null, 2), () -> OptionalInt.of(8)));
        final Map<String, List<String>> solr9 = toMap(params(config(null, 2), () -> OptionalInt.of(9)));
        assertEquals(Arrays.asList("2"), solr8.remove(MAX_SHARDS_PER_NODE));
        assertEquals(solr9, solr8);
        assertEquals(Arrays.asList("2"), solr9.get("numShards"));
        assertEquals(Arrays.asList("janusgraph-configset"), solr9.get("collection.configName"));
    }

    private static Map<String, List<String>> toMap(SolrParams params) {
        final Map<String, List<String>> map = new HashMap<>();
        params.getParameterNamesIterator().forEachRemaining(name -> map.put(name, Arrays.asList(params.getParams(name))));
        return map;
    }
}
