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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.MultiQueryPropertiesStrategyMode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Reads many properties of single cardinality of many vertices in one batch. The CQL backend reads the properties of a
 * vertex which each are one column with `column1 IN ?` queries of up to `storage.cql.grouping.slice-limit` (20)
 * columns. Before JanusGraph 1.2.0 the properties beyond the first 20 were read with one query each.
 * <p>
 * The Cassandra node defaults to the one the benchmark runner starts; {@code -Dbench.cql.host}, {@code -Dbench.cql.port}
 * and {@code -Dbench.cql.dc} point the benchmark at another.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CQLManySinglePropertiesBenchmark {

    @Param({"10", "50"})
    int properties;

    @Param({"1000"})
    int vertices;

    JanusGraph graph;
    String[] keys;

    @Setup
    public void setUp() {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "cql");
        config.set(GraphDatabaseConfiguration.STORAGE_HOSTS, new String[]{System.getProperty("bench.cql.host", "127.0.0.1")});
        config.set(GraphDatabaseConfiguration.STORAGE_PORT, Integer.getInteger("bench.cql.port", 9042));
        config.set(CQLConfigOptions.LOCAL_DATACENTER, System.getProperty("bench.cql.dc", "dc1"));
        config.set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
        config.set(GraphDatabaseConfiguration.PROPERTIES_BATCH_MODE, MultiQueryPropertiesStrategyMode.REQUIRED_PROPERTIES_ONLY.getConfigName());
        config.set(GraphDatabaseConfiguration.PROPERTY_PREFETCHING, false);
        graph = JanusGraphFactory.open(config.getConfiguration());

        final JanusGraphManagement management = graph.openManagement();
        final PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();
        keys = new String[properties];
        for (int i = 0; i < properties; i++) {
            keys[i] = "property" + i;
            management.makePropertyKey(keys[i]).dataType(String.class).cardinality(Cardinality.SINGLE).make();
        }
        management.buildIndex("byName", Vertex.class).addKey(name).buildCompositeIndex();
        management.commit();

        for (int v = 0; v < vertices; v++) {
            final Vertex vertex = graph.addVertex("name", "vertex");
            for (String key : keys) {
                vertex.property(key, "value of " + key);
            }
            if (v % 100 == 99) {
                graph.tx().commit();
            }
        }
        graph.tx().commit();
    }

    @TearDown
    public void tearDown() throws BackendException {
        JanusGraphFactory.drop(graph);
    }

    @Benchmark
    public List<Object> valuesOfEveryProperty() {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            return tx.traversal().V().has("name", "vertex").barrier(Integer.MAX_VALUE).values(keys).toList();
        } finally {
            tx.rollback();
        }
    }
}
