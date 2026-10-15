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
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Commits transactions on BerkeleyJE which add vertices of new values of a unique composite index, and which create
 * property keys. Where the index's consistency is LOCK, a commit locks the index entry of each value it adds and checks
 * that the entry doesn't exist; a schema name is always locked. The values all differ, so the locks don't contend. Run
 * it with {@code -t 8} as well, to commit in several threads at once.
 * <p>
 * BerkeleyJE syncs each commit to disk by default, which takes most of the time of a small commit; with the durability
 * {@code WRITE_NO_SYNC}, a commit only writes its log to the file system.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BerkeleyJELockBenchmark {

    @State(Scope.Benchmark)
    public static class Vertices {

        @Param({"true", "false"})
        boolean locked;

        @Param({"1", "100"})
        int verticesPerTransaction;

        @Param({"default", "WRITE_NO_SYNC"})
        String durability;

        Path directory;
        JanusGraph graph;
        final AtomicLong names = new AtomicLong();

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("berkeleyje-lock-benchmark");
            graph = open(directory, durability);
            JanusGraphManagement mgmt = graph.openManagement();
            PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
            JanusGraphIndex byName = mgmt.buildIndex("byName", Vertex.class).addKey(name).unique()
                .buildCompositeIndex();
            mgmt.setConsistency(byName, locked ? ConsistencyModifier.LOCK : ConsistencyModifier.DEFAULT);
            mgmt.commit();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws BackendException, IOException {
            drop(graph, directory);
        }
    }

    @State(Scope.Benchmark)
    public static class Schema {

        @Param({"default", "WRITE_NO_SYNC"})
        String durability;

        Path directory;
        JanusGraph graph;
        final AtomicLong names = new AtomicLong();

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("berkeleyje-lock-benchmark");
            graph = open(directory, durability);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws BackendException, IOException {
            drop(graph, directory);
        }
    }

    private static JanusGraph open(Path directory, String durability) {
        JanusGraphFactory.Builder builder = JanusGraphFactory.build().set("storage.backend", "berkeleyje")
            .set("storage.directory", directory.toString());
        if (!durability.equals("default")) {
            //The policies of the replicas don't apply to a standalone environment
            builder.set("storage.berkeleyje.ext.je.txn.durability", durability + ",NO_SYNC,NONE");
        }
        return builder.open();
    }

    private static void drop(JanusGraph graph, Path directory) throws BackendException, IOException {
        JanusGraphFactory.drop(graph);
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.delete(path);
            }
        }
    }

    @Benchmark
    public void addVerticesOfNewValues(Vertices state) {
        JanusGraphTransaction tx = state.graph.newTransaction();
        for (int i = 0; i < state.verticesPerTransaction; i++) {
            tx.addVertex("name", "v" + state.names.getAndIncrement());
        }
        tx.commit();
    }

    @Benchmark
    public void createPropertyKey(Schema state) {
        JanusGraphManagement mgmt = state.graph.openManagement();
        mgmt.makePropertyKey("key" + state.names.getAndIncrement()).dataType(String.class).make();
        mgmt.commit();
    }
}
