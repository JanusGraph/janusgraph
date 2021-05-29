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

package org.janusgraph.graphdb.server;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JanusGraphServerTest {
    @Test
    public void testGremlinServerIsCorrectlyLoadedWithExpectedGraphs() {
        final JanusGraphServer server = new JanusGraphServer("src/test/resources/janusgraph-server-with-serializers.yaml");

        CompletableFuture<Void> start = server.start();

        assertFalse(start.isCompletedExceptionally());

        GremlinServer gremlinServer = server.getGremlinServer();
        StandardJanusGraph graph = (StandardJanusGraph) gremlinServer.getServerGremlinExecutor().getGraphManager().getGraph("graph");
        assertNotNull(graph);

        CompletableFuture<Void> stop = server.stop();
        CompletableFuture.allOf(start, stop).join();
    }

    @Test
    public void testSerializersAreConfigured() {
        final JanusGraphServer server = new JanusGraphServer("src/test/resources/janusgraph-server-without-serializers.yaml");

        CompletableFuture<Void> start = server.start();

        assertFalse(start.isCompletedExceptionally());

        Settings settings = server.getJanusGraphSettings();
        assertEquals(5, settings.serializers.size());

        CompletableFuture<Void> stop = server.stop();
        CompletableFuture.allOf(start, stop).join();
    }

    @Test
    public void testGrpcServerIsEnabled() {
        final JanusGraphServer server = new JanusGraphServer("src/test/resources/janusgraph-server-with-grpc.yaml");

        CompletableFuture<Void> start = server.start();

        assertFalse(start.isCompletedExceptionally());

        JanusGraphSettings settings = server.getJanusGraphSettings();
        assertTrue(settings.getGrpcServer().isEnabled());

        CompletableFuture<Void> stop = server.stop();
        CompletableFuture.allOf(start, stop).join();
    }

    @Test
    public void testInvalidConfigurationInitializeFails() {
        final JanusGraphServer server = new JanusGraphServer("src/test/resources/invalid-config.yaml");

        CompletableFuture<Void> start = server.start();

        assertTrue(start.isCompletedExceptionally());
    }

    @Test
    public void testAllowCallStopIfInitializeFails() {
        final JanusGraphServer server = new JanusGraphServer("src/test/resources/invalid-config.yaml");
        CompletableFuture<Void> start = server.start();

        CompletableFuture<Void> stop = server.stop();

        CompletableFuture.allOf(stop).join();
        assertFalse(stop.isCompletedExceptionally());
        assertTrue(start.isCompletedExceptionally());
    }

    @Test
    public void testStartJanusGraphServer() {
        final JanusGraphServer server = new JanusGraphServer("src/test/resources/janusgraph-server-with-serializers.yaml");

        CompletableFuture<Void> start = server.start();

        CompletableFuture<Void> stop = server.stop();
        CompletableFuture.allOf(start, stop).join();

        assertFalse(start.isCompletedExceptionally());
        assertFalse(stop.isCompletedExceptionally());
    }
}
