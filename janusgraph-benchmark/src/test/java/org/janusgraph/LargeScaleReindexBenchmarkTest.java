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

package org.janusgraph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Large-scale mixed-index reindex benchmark: seeds a CQL keyspace with 10 million vertices - each
 * carrying 5 mixed-index properties ("name" + 4 others), 6 non-indexed properties, a real vertex
 * label and a long-tailed number of edges (most vertices 0-3, supernodes up to 5000) - then times
 * {@code SchemaAction.REINDEX} of a freshly created 5-key Elasticsearch mixed index over that data
 * and asserts the document count is exact.
 *
 * <p>DISABLED BY DEFAULT: seeding ~10M vertices / ~40M edges and reindexing them takes tens of
 * minutes and needs a reachable Cassandra and Elasticsearch, so the test only runs when explicitly
 * enabled:
 * <pre>
 *   mvn -pl janusgraph-benchmark test -Dtest=LargeScaleReindexBenchmarkTest \
 *       -Dtest.extra.jvm.opts="-Xmx6g -Dbench.large.enabled=true \
 *           -Dbench.cql.host=127.0.0.1 -Dbench.cql.port=9042 -Dbench.cql.dc=datacenter1 \
 *           -Dbench.es.host=127.0.0.1 -Dbench.es.port=9200"
 * </pre>
 * ({@code test.extra.jvm.opts} is appended to this module's surefire argLine; the -Xmx override
 * matters because the default test heap is 768m while the scan pipeline of a large reindex buffers
 * tens of thousands of rows.)
 * The seeded keyspace ({@code reindexbench10m}) is reused by subsequent runs (a graph-variable
 * marker records the data shape; on mismatch the keyspace is dropped and reseeded), so only the
 * first invocation pays the seeding cost. Scale down with {@code -Dbench.large.size=1000000} for a
 * quicker (still slow) run. Add {@code -Dbench.large.reference=true} to also time the
 * single-pipeline configuration (parallel ranges and the per-partition-limit pushdown disabled) on
 * the same data, which shows the gain of the parallel scan on wide real-world-shaped graphs.
 *
 * <p>All {@code bench.*} system properties of {@link ReindexThroughputBenchmark} are honored;
 * this test only fills in large-scale defaults for whatever is unset.
 */
@EnabledIfSystemProperty(named = "bench.large.enabled", matches = "true")
public class LargeScaleReindexBenchmarkTest {

    @Test
    public void reindexLargeGraphWithParallelScan() throws Exception {
        applyLargeScaleDefaults();
        // The optimized scan configuration under test (overridable from the command line).
        setIfAbsent("bench.ranges", "8");
        setIfAbsent("bench.scanPageSize", "2000");

        runAndAssert("parallel-scan");
    }

    /**
     * Times the same reindex with the parallel token-range scan and the PER PARTITION LIMIT
     * pushdown disabled (single scan pipeline), for a before/after comparison on identical data.
     * Requires {@code -Dbench.large.reference=true} in addition to {@code bench.large.enabled}.
     */
    @Test
    @EnabledIfSystemProperty(named = "bench.large.reference", matches = "true")
    public void reindexLargeGraphSinglePipelineReference() throws Exception {
        applyLargeScaleDefaults();
        final Map<String, String> forced = new HashMap<>();
        forced.put("bench.ranges", "1");
        forced.put("bench.scanPageSize", "0");
        forced.put("bench.perPartitionLimit", "false");

        final Map<String, String> saved = new HashMap<>();
        forced.forEach((key, value) -> {
            saved.put(key, System.getProperty(key));
            System.setProperty(key, value);
        });
        try {
            runAndAssert("single-pipeline reference");
        } finally {
            saved.forEach((key, value) -> {
                if (value == null) System.clearProperty(key); else System.setProperty(key, value);
            });
        }
    }

    private void runAndAssert(String label) throws Exception {
        final List<ReindexThroughputBenchmark.Result> results = ReindexThroughputBenchmark.runFromSystemProperties();
        assertFalse(results.isEmpty(), "expected at least one reindex run");
        for (ReindexThroughputBenchmark.Result result : results) {
            System.out.printf("%s: index=%s reindexed %d vertices in %.1fs (%.0f vertices/s, %.0f/min)%n",
                label, result.indexName, result.size, result.seconds,
                result.size / result.seconds, result.size / result.seconds * 60d);
            assertEquals(0, result.failure, "scan rows failed during reindex");
            assertEquals(result.size, result.docUpdates,
                "document count must equal the vertex count (a mismatch means the scan lost or duplicated keys)");
        }
    }

    private static void applyLargeScaleDefaults() {
        setIfAbsent("bench.size", System.getProperty("bench.large.size", "10000000"));
        setIfAbsent("bench.keyspace", "reindexbench10m");
        setIfAbsent("bench.extraProps", "10");     // 4 of them indexed (+ "name" = 5 mixed-index keys), 6 non-indexed
        setIfAbsent("bench.indexedProps", "4");
        setIfAbsent("bench.edges.skewed", "true"); // long-tailed 0..bench.edges.max out-degree
        setIfAbsent("bench.edges.max", "5000");
        setIfAbsent("bench.threads", "100");
        setIfAbsent("bench.pageSize", "200");
        setIfAbsent("bench.batchSize", "1000");
    }

    private static void setIfAbsent(String key, String value) {
        if (System.getProperty(key) == null) {
            System.setProperty(key, value);
        }
    }
}
