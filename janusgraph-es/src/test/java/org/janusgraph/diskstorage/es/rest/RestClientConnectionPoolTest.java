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

package org.janusgraph.diskstorage.es.rest;

import com.sun.net.httpserver.HttpServer;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.es.ElasticSearchMutation;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Elasticsearch client against local servers which hold every search until the test releases them, so that the
 * searches in flight at once show how many connections the client opens to each host.
 */
public class RestClientConnectionPoolTest {

    private static final String INDEX_NAME = "pool";

    private static final byte[] EMPTY_SEARCH_RESPONSE = "{\"took\":1,\"hits\":{\"hits\":[]}}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BULK_RESPONSE = "{\"took\":1,\"errors\":false,\"items\":[]}".getBytes(StandardCharsets.UTF_8);

    private final CountDownLatch release = new CountDownLatch(1);
    private final List<HeldServer> servers = new ArrayList<>();

    //A host which holds every search until the test releases it, and counts the searches it holds at once
    private final class HeldServer {
        private final HttpServer server;
        private final ExecutorService threads = Executors.newCachedThreadPool();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger mostInFlight = new AtomicInteger();
        private volatile String bulkContentEncoding;
        private volatile String searchAcceptEncoding;

        HeldServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 1_000);
            server.setExecutor(threads);
            server.createContext("/", exchange -> {
                exchange.getRequestBody().readAllBytes();
                final byte[] response;
                if (exchange.getRequestURI().getPath().endsWith("/_bulk")) {
                    bulkContentEncoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
                    response = BULK_RESPONSE;
                } else {
                    searchAcceptEncoding = exchange.getRequestHeaders().getFirst("Accept-Encoding");
                    mostInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                    try {
                        release.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    inFlight.decrementAndGet();
                    response = EMPTY_SEARCH_RESPONSE;
                }
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
            });
            server.start();
            servers.add(this);
        }

        String host() {
            return "127.0.0.1:" + server.getAddress().getPort();
        }

        void stop() {
            server.stop(0);
            threads.shutdownNow();
        }
    }

    @AfterEach
    public void stopServers() {
        release.countDown();
        servers.forEach(HeldServer::stop);
    }

    private static ElasticSearchClient connect(List<HeldServer> hosts, Map<String, String> options) throws IOException {
        final CommonsConfiguration cc = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc.set("index." + INDEX_NAME + ".backend", "elasticsearch");
        cc.set("index." + INDEX_NAME + ".hostname", hosts.stream().map(HeldServer::host).collect(Collectors.joining(",")));
        cc.set("index." + INDEX_NAME + ".elasticsearch.major-version", "9");
        options.forEach((key, value) -> cc.set("index." + INDEX_NAME + ".elasticsearch." + key, value));
        final ModifiableConfiguration config = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, cc,
            BasicConfiguration.Restriction.NONE);
        return new RestClientSetup().connect(config.restrictTo(INDEX_NAME));
    }

    //Sends the searches at once, waits until the hosts hold as many as expected, makes sure no more arrive, and
    //returns how many each host held at most
    private List<Integer> mostSearchesInFlight(List<HeldServer> hosts, Map<String, String> options, int searches,
                                               int expected) throws Exception {
        try (ElasticSearchClient client = connect(hosts, options)) {
            final ExecutorService callers = Executors.newFixedThreadPool(searches);
            try {
                final List<Future<?>> results = new ArrayList<>();
                for (int i = 0; i < searches; i++) {
                    results.add(callers.submit(() -> client.search("janusgraph_pool", Collections.singletonMap("size", 1), false)));
                }
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (hosts.stream().mapToInt(host -> host.inFlight.get()).sum() < expected && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                //The searches which the pool lets through arrive within milliseconds of each other
                Thread.sleep(300);
                final List<Integer> most = hosts.stream().map(host -> host.mostInFlight.get()).collect(Collectors.toList());
                release.countDown();
                for (Future<?> result : results) {
                    result.get(20, TimeUnit.SECONDS);
                }
                return most;
            } finally {
                callers.shutdownNow();
            }
        }
    }

    //The Elasticsearch client on its own would let 10 searches through to a host
    @Test
    public void shouldLetASingleHostTakeAllThirtyConnectionsByDefault() throws Exception {
        assertEquals(Collections.singletonList(30),
            mostSearchesInFlight(Collections.singletonList(new HeldServer()), Collections.emptyMap(), 40, 30));
    }

    //Each of two hosts gets half of the 30, so that a host which stops answering can't hold every connection
    @Test
    public void shouldShareTheConnectionsAmongTheHostsByDefault() throws Exception {
        assertEquals(Arrays.asList(15, 15),
            mostSearchesInFlight(Arrays.asList(new HeldServer(), new HeldServer()), Collections.emptyMap(), 60, 30));
    }

    @Test
    public void shouldOpenAsManyConnectionsToAHostAsConfigured() throws Exception {
        assertEquals(Collections.singletonList(5), mostSearchesInFlight(Collections.singletonList(new HeldServer()),
            Collections.singletonMap("max-connections-per-host", "5"), 20, 5));
    }

    @Test
    public void shouldOpenNoMoreConnectionsToAllHostsThanConfigured() throws Exception {
        assertEquals(Collections.singletonList(4), mostSearchesInFlight(Collections.singletonList(new HeldServer()),
            Collections.singletonMap("max-connections", "4"), 20, 4));
    }

    @Test
    public void shouldLetASingleHostTakeAllConnectionsWhenMoreAreConfigured() throws Exception {
        assertEquals(Collections.singletonList(40), mostSearchesInFlight(Collections.singletonList(new HeldServer()),
            Collections.singletonMap("max-connections", "40"), 40, 40));
    }

    @Test
    public void shouldCompressRequestsAndAcceptCompressedResponsesWhenConfigured() throws Exception {
        release.countDown();
        final HeldServer server = new HeldServer();
        try (ElasticSearchClient client = connect(Collections.singletonList(server), Collections.singletonMap("compression", "true"))) {
            client.bulkRequest(Collections.singletonList(
                ElasticSearchMutation.createDeleteRequest("janusgraph_pool", "pool", "doc")), null);
            client.search("janusgraph_pool", Collections.singletonMap("size", 1), false);
        }
        assertEquals("gzip", server.bulkContentEncoding);
        assertTrue(server.searchAcceptEncoding != null && server.searchAcceptEncoding.contains("gzip"),
            server.searchAcceptEncoding);
    }

    @Test
    public void shouldNotCompressRequestsByDefault() throws Exception {
        release.countDown();
        final HeldServer server = new HeldServer();
        try (ElasticSearchClient client = connect(Collections.singletonList(server), Collections.emptyMap())) {
            client.bulkRequest(Collections.singletonList(
                ElasticSearchMutation.createDeleteRequest("janusgraph_pool", "pool", "doc")), null);
        }
        assertNull(server.bulkContentEncoding);
    }
}
