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

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.server.util.VirtualThreads;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.graphdb.server.util.VirtualThreadsTest.isVirtual;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JanusGraphServerTest {

    private static volatile CountDownLatch requestHeld;
    private static volatile CountDownLatch requestReleased;

    /**
     * Holds the Gremlin script which calls it until the test releases it.
     */
    public static boolean holdRequest() throws InterruptedException {
        requestHeld.countDown();
        return requestReleased.await(30, TimeUnit.SECONDS);
    }
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
        assertEquals(3, settings.serializers.size());

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
    public void testGremlinPoolOfPlatformThreadsByDefault() throws Exception {
        final JanusGraphServer server = new JanusGraphServer("src/test/resources/janusgraph-server-without-serializers.yaml");
        server.start().join();
        try {
            assertFalse(server.getJanusGraphSettings().isGremlinPoolVirtualThreads());
            ExecutorService pool = server.getGremlinServer().getServerGremlinExecutor().getGremlinExecutorService();
            Thread thread = pool.submit(Thread::currentThread).get(10, TimeUnit.SECONDS);
            assertFalse(isVirtual(thread));
            assertTrue(thread.getName().startsWith("gremlin-server-exec-"), thread.getName());
        } finally {
            server.stop().join();
        }
    }

    @Test
    @EnabledForJreRange(minVersion = VirtualThreads.MIN_JAVA_VERSION)
    public void testGremlinPoolOfVirtualThreads() throws Exception {
        final JanusGraphServer server = new JanusGraphServer("src/test/resources/janusgraph-server-with-virtual-threads.yaml");
        server.start().join();
        final Cluster cluster = Cluster.build("localhost").port(8182).create();
        requestHeld = new CountDownLatch(1);
        requestReleased = new CountDownLatch(1);
        try {
            ThreadPoolExecutor pool = (ThreadPoolExecutor)
                server.getGremlinServer().getServerGremlinExecutor().getGremlinExecutorService();
            assertEquals(1, pool.getMaximumPoolSize());
            assertEquals(1, pool.getQueue().remainingCapacity());

            Client client = cluster.connect();
            assertTrue(client.submit("Thread.currentThread().isVirtual()").one().getBoolean());
            assertTrue(client.submit("Thread.currentThread().getName()").one().getString()
                .startsWith("gremlin-server-exec-"));

            // the one thread runs a request and a second request waits, so a third one is rejected
            CompletableFuture<List<Result>> held = client.submitAsync(getClass().getName() + ".holdRequest()")
                .thenCompose(ResultSet::all);
            assertTrue(requestHeld.await(10, TimeUnit.SECONDS));
            CompletableFuture<List<Result>> waiting = client.submitAsync("1 + 1").thenCompose(ResultSet::all);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (pool.getQueue().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, pool.getQueue().size());
            ExecutionException rejected = assertThrows(ExecutionException.class,
                () -> client.submit("2 + 2").all().get(10, TimeUnit.SECONDS));
            ResponseException response = assertInstanceOf(ResponseException.class, rejected.getCause());
            assertEquals(ResponseStatusCode.TOO_MANY_REQUESTS, response.getResponseStatusCode());

            requestReleased.countDown();
            assertTrue(held.get(10, TimeUnit.SECONDS).get(0).getBoolean());
            assertEquals(2, waiting.get(10, TimeUnit.SECONDS).get(0).getInt());
        } finally {
            requestReleased.countDown();
            cluster.close();
            server.stop().join();
        }
    }

    @Test
    @EnabledForJreRange(maxVersion = VirtualThreads.MIN_JAVA_VERSION - 1)
    public void testGremlinPoolOfVirtualThreadsRefusedBeforeJava24() {
        final JanusGraphServer server = new JanusGraphServer("src/test/resources/janusgraph-server-with-virtual-threads.yaml");

        CompletableFuture<Void> start = server.start();

        CompletionException e = assertThrows(CompletionException.class, start::join);
        IllegalStateException cause = assertInstanceOf(IllegalStateException.class, e.getCause());
        assertTrue(cause.getMessage().startsWith("gremlinPoolVirtualThreads requires Java 24 or later"),
            cause.getMessage());
        assertNull(server.getGremlinServer());
        server.stop().join();
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
