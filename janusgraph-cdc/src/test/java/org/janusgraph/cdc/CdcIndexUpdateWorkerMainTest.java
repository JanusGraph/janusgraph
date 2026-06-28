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

package org.janusgraph.cdc;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CdcIndexUpdateWorkerMainTest {

    @TempDir
    Path tempDir;

    private CdcWorkerConfiguration config(int workerThreads) {
        return CdcWorkerConfiguration.builder()
            .bootstrapServers("dummy:9092")
            .topics(Arrays.asList("cassandra.janusgraph.edgestore"))
            .workerThreads(workerThreads)
            .pollTimeout(Duration.ofMillis(10))
            .build();
    }

    @Test
    public void cdcEnabledBackingIndexesReflectsConfig() throws InterruptedException {
        ModifiableConfiguration cfg = GraphDatabaseConfiguration.buildGraphConfiguration();
        cfg.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        cfg.set(GraphDatabaseConfiguration.INDEX_BACKEND, "lucene", "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, tempDir.resolve("lucene").toString(), "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, "search");
        JanusGraph graph = JanusGraphFactory.open(cfg.getConfiguration());
        try {
            JanusGraphManagement mgmt = graph.openManagement();
            PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
            mgmt.buildIndex("vsearch", Vertex.class).addKey(name).buildMixedIndex("search");
            mgmt.commit();
            org.janusgraph.graphdb.database.management.ManagementSystem
                .awaitGraphIndexStatus(graph, "vsearch").status(SchemaStatus.ENABLED).timeout(60, ChronoUnit.SECONDS).call();

            Set<String> cdcIndexes = CdcIndexUpdateWorkerMain.cdcEnabledBackingIndexes((StandardJanusGraph) graph);
            assertEquals(Collections.singleton("search"), cdcIndexes);
        } finally {
            graph.close();
        }
    }

    @Test
    public void buildsConfiguredNumberOfWorkersAndClosesGraph() throws InterruptedException {
        ModifiableConfiguration cfg = GraphDatabaseConfiguration.buildGraphConfiguration();
        cfg.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        cfg.set(GraphDatabaseConfiguration.INDEX_BACKEND, "lucene", "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, tempDir.resolve("lucene").toString(), "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, "search");
        JanusGraph graph = JanusGraphFactory.open(cfg.getConfiguration());
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("vsearch", Vertex.class).addKey(name).buildMixedIndex("search");
        mgmt.commit();
        org.janusgraph.graphdb.database.management.ManagementSystem
            .awaitGraphIndexStatus(graph, "vsearch").status(SchemaStatus.ENABLED).timeout(60, ChronoUnit.SECONDS).call();

        CdcIndexUpdateWorkerMain main = new CdcIndexUpdateWorkerMain(graph, config(3),
            () -> new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        assertEquals(3, main.getWorkerCount());

        main.start();
        main.close(); // stops workers and closes the graph
        assertFalse(graph.isOpen(), "graph closed on shutdown");
    }

    @Test
    public void refusesToStartWithNoCdcEnabledIndex() {
        // No index has cdc.enabled=true -> a running worker would consume and commit change events while applying
        // nothing, permanently discarding those updates. The runner must fail fast instead of silently no-op'ing.
        ModifiableConfiguration cfg = GraphDatabaseConfiguration.buildGraphConfiguration();
        cfg.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        JanusGraph graph = JanusGraphFactory.open(cfg.getConfiguration());
        try {
            assertThrows(IllegalStateException.class, () -> new CdcIndexUpdateWorkerMain(graph, config(1),
                () -> new MockConsumer<>(OffsetResetStrategy.EARLIEST)));
        } finally {
            graph.close();
        }
    }

    @Test
    public void closesAlreadyCreatedConsumersWhenConstructionFails() throws InterruptedException {
        ModifiableConfiguration cfg = GraphDatabaseConfiguration.buildGraphConfiguration();
        cfg.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        cfg.set(GraphDatabaseConfiguration.INDEX_BACKEND, "lucene", "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, tempDir.resolve("lucene-ctorfail").toString(), "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, "search");
        JanusGraph graph = JanusGraphFactory.open(cfg.getConfiguration());
        try {
            JanusGraphManagement mgmt = graph.openManagement();
            PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
            mgmt.buildIndex("vsearch", Vertex.class).addKey(name).buildMixedIndex("search");
            mgmt.commit();
            org.janusgraph.graphdb.database.management.ManagementSystem
                .awaitGraphIndexStatus(graph, "vsearch").status(SchemaStatus.ENABLED).timeout(60, ChronoUnit.SECONDS).call();

            java.util.List<MockConsumer<byte[], byte[]>> created = new java.util.ArrayList<>();
            assertThrows(RuntimeException.class, () -> new CdcIndexUpdateWorkerMain(graph, config(2), () -> {
                if (!created.isEmpty()) {
                    throw new RuntimeException("simulated consumer construction failure");
                }
                MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
                created.add(consumer);
                return consumer;
            }));
            assertEquals(1, created.size());
            assertTrue(created.get(0).closed(), "consumer created before the failure is closed, not leaked");
        } finally {
            graph.close();
        }
    }

    @Test
    public void refusesToStartWhenCdcEnabledButNoMixedIndexExists() {
        // index.search.cdc.enabled=true but NO mixed index is built on "search": the applier manages nothing, so the
        // worker would still consume and commit change events while applying nothing. The runner must fail fast.
        ModifiableConfiguration cfg = GraphDatabaseConfiguration.buildGraphConfiguration();
        cfg.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "inmemory");
        cfg.set(GraphDatabaseConfiguration.INDEX_BACKEND, "lucene", "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_DIRECTORY, tempDir.resolve("lucene-nomixed").toString(), "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, "search");
        JanusGraph graph = JanusGraphFactory.open(cfg.getConfiguration());
        try {
            assertThrows(IllegalStateException.class, () -> new CdcIndexUpdateWorkerMain(graph, config(1),
                () -> new MockConsumer<>(OffsetResetStrategy.EARLIEST)));
        } finally {
            graph.close();
        }
    }
}
