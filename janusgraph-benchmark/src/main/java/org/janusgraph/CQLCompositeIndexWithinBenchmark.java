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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.cql.CQLConfigOptions;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Looks up several values of a composite index, as {@code has(key, within(values))} does. Before JanusGraph 1.2.0 each
 * value was one more round trip to Cassandra, made after the previous one returned.
 * <p>
 * The Cassandra node defaults to the one the benchmark runner starts; {@code -Dbench.cql.host}, {@code -Dbench.cql.port}
 * and {@code -Dbench.cql.dc} point the benchmark at another.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class CQLCompositeIndexWithinBenchmark {

    @Param({"10", "100", "1000"})
    int values;

    JanusGraph graph;
    List<Long> uids;
    List<String> groups;

    @Setup
    public void setUp() {
        final ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "cql");
        config.set(GraphDatabaseConfiguration.STORAGE_HOSTS, new String[]{System.getProperty("bench.cql.host", "127.0.0.1")});
        config.set(GraphDatabaseConfiguration.STORAGE_PORT, Integer.getInteger("bench.cql.port", 9042));
        config.set(CQLConfigOptions.LOCAL_DATACENTER, System.getProperty("bench.cql.dc", "dc1"));
        graph = JanusGraphFactory.open(config.getConfiguration());

        final JanusGraphManagement management = graph.openManagement();
        final PropertyKey uid = management.makePropertyKey("uid").dataType(Long.class).make();
        final PropertyKey group = management.makePropertyKey("group").dataType(String.class).make();
        management.buildIndex("byUid", Vertex.class).addKey(uid).unique().buildCompositeIndex();
        management.buildIndex("byGroup", Vertex.class).addKey(group).buildCompositeIndex();
        management.commit();

        uids = new ArrayList<>(values);
        groups = new ArrayList<>(values);
        for (int i = 0; i < values; i++) {
            graph.addVertex("uid", (long) i, "group", "group" + i);
            uids.add((long) i);
            groups.add("group" + i);
            if (i % 1000 == 999) {
                graph.tx().commit();
            }
        }
        graph.tx().commit();
    }

    @TearDown
    public void tearDown() throws BackendException {
        JanusGraphFactory.drop(graph);
    }

    /**
     * Values of a unique index, as a lookup by an external id has.
     */
    @Benchmark
    public List<Vertex> uniqueValues() {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            return tx.traversal().V().has("uid", P.within(uids)).toList();
        } finally {
            tx.rollback();
        }
    }

    @Benchmark
    public List<Vertex> uniqueValuesWithALimit() {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            return tx.traversal().V().has("uid", P.within(uids)).limit(values).toList();
        } finally {
            tx.rollback();
        }
    }

    @Benchmark
    public List<Vertex> valuesOfAnIndexWhichIsNotUnique() {
        final JanusGraphTransaction tx = graph.newTransaction();
        try {
            return tx.traversal().V().has("group", P.within(groups)).toList();
        } finally {
            tx.rollback();
        }
    }
}
