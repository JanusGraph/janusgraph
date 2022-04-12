// Copyright 2022 JanusGraph Authors
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

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;

@Disabled
public class BerkeleyInterruptionTest {
    JanusGraph graph;

    @BeforeEach
    void setUp() {
        final ModifiableConfiguration config = BerkeleyStorageSetup.getBerkeleyJEConfiguration();
        final String dir = config.get(STORAGE_DIRECTORY);
        FileUtils.deleteQuietly(new File(dir));
        graph = JanusGraphFactory.open(config);
    }

    @AfterEach
    void tearDown() {
        graph.close();
    }

    @RepeatedTest(5)
    public void test() throws InterruptedException {
        for (int i = 0; i < 5000; i++) {
            graph.traversal()
                .addV("V").property("a", "bb" + i).property("b", "bb" + i)
                .addV("V").property("a", "bb" + i).property("b", "bb" + i)
                .addV("V").property("a", "bb" + i).property("b", "bb" + i)
                .iterate();
            if (i % 10_000 == 0) {
                graph.tx().commit();
            }
        }
        graph.tx().commit();

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            graph.traversal().V()
                .elementMap()
                .count().next();
        }, executorService);

        Thread.sleep(200);
        executorService.shutdownNow();

        try {
            future.get();
        } catch (ExecutionException e) {
            Assertions.assertEquals(TraversalInterruptedException.class, e.getCause().getClass(), e.getMessage());
        }

        try {
            Assertions.assertEquals(15000, graph.traversal().V().count().next());
        } catch (JanusGraphException e) {
            Assertions.fail("bdb should be reopened");
        }

        Assertions.assertEquals(15000, graph.traversal().V().count().next());
    }
}
