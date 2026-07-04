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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.index.MixedIndexUpdateApplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/**
 * Standalone runner for the CDC index-update worker(s). Opens a JanusGraph instance (read access for reindex-from-
 * current-state plus index-backend write access), wires the Debezium decoder and {@link MixedIndexUpdateApplier} for
 * the CDC-enabled mixed indexes, and starts {@code cdc.worker-threads} workers in one consumer group. Run several
 * processes pointed at the same Kafka topic/group to scale out horizontally.
 *
 * <pre>java org.janusgraph.cdc.CdcIndexUpdateWorkerMain cdc.properties</pre>
 *
 * where {@code cdc.properties} contains the {@code cdc.*} settings (see {@link CdcWorkerConfiguration}) plus
 * {@code cdc.graph-config} pointing at the JanusGraph configuration file.
 */
public final class CdcIndexUpdateWorkerMain implements AutoCloseable {

    public static final String GRAPH_CONFIG = CdcWorkerConfiguration.GRAPH_CONFIG;

    private static final Logger log = LoggerFactory.getLogger(CdcIndexUpdateWorkerMain.class);

    private final JanusGraph graph;
    private final List<CdcIndexUpdateWorker> workers;

    public CdcIndexUpdateWorkerMain(JanusGraph graph, CdcWorkerConfiguration config,
                                    Supplier<Consumer<byte[], byte[]>> consumerFactory) {
        if (!(graph instanceof StandardJanusGraph)) {
            throw new IllegalArgumentException("The CDC index-update worker requires a StandardJanusGraph instance "
                + "(reindex-from-current-state needs its index serializer and schema access), but got: "
                + (graph == null ? "null" : graph.getClass().getName()));
        }
        this.graph = graph;
        final StandardJanusGraph standardGraph = (StandardJanusGraph) graph;
        final Set<String> cdcIndexes = cdcEnabledBackingIndexes(standardGraph);
        final MixedIndexUpdateApplier applier = new MixedIndexUpdateApplier(standardGraph, cdcIndexes);
        if (applier.getManagedBackingIndexes().isEmpty()) {
            throw new IllegalStateException("No CDC-managed mixed indexes found; refusing to start the CDC "
                + "index-update worker. Ensure at least one backing index has index.[<name>].cdc.enabled=true AND that "
                + "at least one mixed index is built on it -- otherwise the worker would consume and commit change "
                + "events while applying nothing, permanently discarding those updates.");
        }
        final DebeziumCassandraJsonDecoder decoder = new DebeziumCassandraJsonDecoder(standardGraph);
        final List<CdcIndexUpdateWorker> created = new ArrayList<>();
        try {
            for (int i = 0; i < config.getWorkerThreads(); i++) {
                created.add(new CdcIndexUpdateWorker(consumerFactory.get(), decoder, applier::apply, config));
            }
        } catch (RuntimeException e) {
            // Don't leak the consumers of the workers already created (close() on a never-started worker releases it).
            created.forEach(CdcIndexUpdateWorker::close);
            throw e;
        }
        this.workers = created;
        log.info("Configured {} CDC index-update worker(s) for backing indexes {}", created.size(), cdcIndexes);
    }

    /** The backing index names with {@code index.[X].cdc.enabled=true}. */
    static Set<String> cdcEnabledBackingIndexes(StandardJanusGraph graph) {
        return GraphDatabaseConfiguration.getCdcBackingIndexNames(graph.getConfiguration().getConfiguration(), false);
    }

    public int getWorkerCount() {
        return workers.size();
    }

    public void start() {
        workers.forEach(CdcIndexUpdateWorker::start);
    }

    @Override
    public void close() {
        // Two-phase shutdown: signal every worker first, then wait for each, so they wind down concurrently and the
        // total shutdown latency is roughly the slowest single worker instead of the sum across workers.
        workers.forEach(CdcIndexUpdateWorker::initiateShutdown);
        workers.forEach(CdcIndexUpdateWorker::awaitTermination);
        final long stillRunning = workers.stream().filter(CdcIndexUpdateWorker::isAlive).count();
        if (stillRunning > 0) {
            // awaitTermination's join is bounded, so a worker stuck in a slow index/storage call can outlive it.
            // Closing the graph is still the right contract for close() -- the stuck worker's in-flight apply will
            // then fail against the closed graph WITHOUT committing its offsets, so Kafka redelivers the batch to
            // the next consumer (at-least-once); nothing is lost, but the errors it logs are expected.
            log.warn("Closing the graph while {} CDC worker(s) have not terminated within the shutdown timeout; "
                + "their in-flight batch will fail harmlessly and be redelivered by Kafka", stillRunning);
        }
        if (graph.isOpen()) {
            graph.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: CdcIndexUpdateWorkerMain <cdc.properties>");
            System.exit(1);
        }
        final Properties props = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get(args[0]))) {
            props.load(in);
        }
        final CdcWorkerConfiguration config = CdcWorkerConfiguration.fromProperties(props);
        final String graphConfig = props.getProperty(GRAPH_CONFIG);
        if (graphConfig == null) {
            throw new IllegalArgumentException(GRAPH_CONFIG + " is required");
        }
        final JanusGraph graph = JanusGraphFactory.open(graphConfig);
        final CdcIndexUpdateWorkerMain runner;
        try {
            runner = new CdcIndexUpdateWorkerMain(graph, config,
                () -> new KafkaConsumer<>(config.toConsumerProperties()));
        } catch (RuntimeException e) {
            graph.close();
            throw e;
        }
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                runner.close();
            } finally {
                shutdownLatch.countDown();
            }
        }));
        runner.start();
        log.info("Started {} CDC index-update worker(s); press Ctrl-C to stop", runner.getWorkerCount());
        // Keep the process alive until a shutdown signal (Ctrl-C / SIGTERM) fires the hook above.
        shutdownLatch.await();
    }
}
