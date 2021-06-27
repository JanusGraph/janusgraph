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

package org.janusgraph.graphdb;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.util.MetricInstrumentedStore;
import org.janusgraph.util.stats.MetricManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.janusgraph.diskstorage.Backend.METRICS_INDEX_PROVIDER_NAME;
import static org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider.M_MIXED_COUNT_QUERY;
import static org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider.M_MUTATE;
import static org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider.M_QUERY;
import static org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider.M_RAW_QUERY;
import static org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider.M_TOTALS;
import static org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider.OPERATION_NAMES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.METRICS_PREFIX_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class IndexMetricTest extends JanusGraphBaseTest {
    public MetricManager metric;
    public String METRICS_INDEX_PREFIX = METRICS_PREFIX_DEFAULT + "." + METRICS_INDEX_PROVIDER_NAME;

    public abstract WriteConfiguration getConfiguration();

    @Override
    public void open(WriteConfiguration config) {
        metric = MetricManager.INSTANCE;
        super.open(config);
    }

    @Test
    public void testIndexMetrics() {
        PropertyKey p1 = mgmt.makePropertyKey("p1").dataType(String.class).make();
        mgmt.makePropertyKey("p2").dataType(String.class).make();
        mgmt.buildIndex("idx", Vertex.class).addKey(p1, Mapping.STRING.asParameter()).buildMixedIndex("search");
        finishSchema();

        graph.traversal().addV().property("p1", "value").iterate();
        graph.tx().commit();
        graph.traversal().V().has("p1", "value").iterate();
        graph.traversal().V().has("p2", "value").iterate();
        graph.traversal().V().has("p1", "value").count().next();
        graph.indexQuery("idx", "p1:*").vertexTotals();
        graph.indexQuery("idx", "p1:*").vertexStream();

        Assertions.assertThrows(JanusGraphException.class,  () ->
            graph.indexQuery("idx", "!@#$%^").vertexTotals());

        verifyIndexMetrics("search", METRICS_INDEX_PREFIX, ImmutableMap.of(M_MUTATE, 1L, M_QUERY, 1L, M_MIXED_COUNT_QUERY, 1L, M_TOTALS, 2L, M_RAW_QUERY, 1L));
        assertEquals(1, metric.getCounter(METRICS_INDEX_PREFIX, "search", M_TOTALS, MetricInstrumentedStore.M_EXCEPTIONS).getCount());
    }

    public void verifyIndexMetrics(String indexName, String prefix, Map<String, Long> operationCounts) {
        for (String operation : OPERATION_NAMES) {
            Long count = operationCounts.get(operation);
            if (count == null) {
                count = 0L;
            }
            assertEquals(count.longValue(), metric.getCounter(prefix, indexName, operation, MetricInstrumentedStore.M_CALLS).getCount(),
                Joiner.on(".").join(prefix, indexName, operation, MetricInstrumentedStore.M_CALLS));
        }
    }
}
