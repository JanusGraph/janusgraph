// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb;

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.TestCategory;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.testutil.JUnitBenchmarkProvider;
import org.janusgraph.testutil.RandomGenerator;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * High concurrency test cases to spot deadlocks and other failures that can occur under high degrees of parallelism.
 */
@Tag(TestCategory.PERFORMANCE_TESTS)
public abstract class JanusGraphConcurrentTest extends JanusGraphBaseTest {

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();

    // Parallelism settings
    private static final int THREAD_COUNT = getThreadCount();
    private static final int TASK_COUNT = THREAD_COUNT * 256;

    // Graph structure settings
    private static final int VERTEX_COUNT = 1000;
    private static final int EDGE_COUNT = 5;
    private static final int REL_COUNT = 5;

    private static final Logger log =
            LoggerFactory.getLogger(JanusGraphConcurrentTest.class);

    private ExecutorService executor;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        executor = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    private void initializeGraph() {
        //Create schema
        for (int i = 0; i < REL_COUNT; i++) {
            makeLabel("rel" + i);
        }
        makeVertexIndexedUniqueKey("uid",Integer.class);
        finishSchema();

        // Generate synthetic graph
        Vertex[] vertices = new Vertex[VERTEX_COUNT];
        for (int i = 0; i < VERTEX_COUNT; i++) {
            vertices[i] = tx.addVertex("uid", i);
        }
        for (int i = 0; i < VERTEX_COUNT; i++) {
            for (int r = 0; r < REL_COUNT; r++) {
                for (int j = 1; j <= EDGE_COUNT; j++) {
                    vertices[i].addEdge("rel"+r, vertices[wrapAround(i + j, VERTEX_COUNT)]);
                }
            }
        }

        // Get a new transaction
        clopen();
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            log.error("Abnormal executor shutdown");
            Thread.dumpStack();
        } else {
            log.debug("Test executor completed normal shutdown");
        }
        super.tearDown();
    }

    @Test
    public void concurrentTxRead() throws Exception {
        final int numTypes = 20;
        final int numThreads = 100;
        for (int i = 0; i < numTypes / 2; i++) {
            if (i%4 == 0) makeVertexIndexedUniqueKey("test"+i, String.class);
            else makeKey("test"+i,String.class);
        }
        for (int i = numTypes / 2; i < numTypes; i++) {
            EdgeLabelMaker tm = mgmt.makeEdgeLabel("test" + i);
            if (i % 4 == 1) tm.unidirected();
            tm.make();
        }
        finishSchema();
        clopen();

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            threads[t] = new Thread(() -> {
                JanusGraphTransaction tx = graph.newTransaction();
                for (int i = 0; i < numTypes; i++) {
                    RelationType type = tx.getRelationType("test" + i);
                    if (i < numTypes / 2) assertTrue(type.isPropertyKey());
                    else assertTrue(type.isEdgeLabel());
                }
                tx.commit();
            });
            threads[t].start();
        }
        for (int t = 0; t < numThreads; t++) {
            threads[t].join();
        }
    }


    /**
     * Insert an extremely simple graph and start
     * TASK_COUNT simultaneous readers in an executor with
     * THREAD_COUNT threads.
     *
     * @throws Exception
     */
    @Test
    public void concurrentReadsOnSingleTransaction() throws Exception {
        initializeGraph();

        PropertyKey id = tx.getPropertyKey("uid");

        // Tail many concurrent readers on a single transaction
        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            int vertexId = RandomGenerator.randomInt(0, VERTEX_COUNT);
            EdgeLabel edgeLabel = tx.getEdgeLabel("rel" + RandomGenerator.randomInt(0, REL_COUNT));
            executor.execute(new SimpleReader(tx, startLatch, stopLatch, vertexId, edgeLabel.name(), EDGE_COUNT * 2, id.name()));
            startLatch.countDown();
        }
        stopLatch.await();
    }

    /**
     * Tail many readers, as in {@link #concurrentReadsOnSingleTransaction()},
     * but also start some threads that add and remove relationships and
     * properties while the readers are working; all tasks share a common
     * transaction.
     * <p>
     * The readers do not look for the properties or relationships the
     * writers are mutating, since this is all happening on a common transaction.
     *
     * @throws Exception
     */
    @Test
    public void concurrentReadWriteOnSingleTransaction() throws Exception {
        initializeGraph();

        mgmt.getPropertyKey("uid");
        makeVertexIndexedUniqueKey("dummyProperty",String.class);
        makeLabel("dummyRelationship");
        finishSchema();

        PropertyKey id = tx.getPropertyKey("uid");
        Runnable propMaker = new RandomPropertyMaker(tx, VERTEX_COUNT, id.name(), "dummyProperty");
        Runnable relMaker = new FixedRelationshipMaker(tx, id.name(), "dummyRelationship");

        Future<?> propFuture = executor.submit(propMaker);
        Future<?> relFuture = executor.submit(relMaker);

        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            int vertexId = RandomGenerator.randomInt(0, VERTEX_COUNT);
            EdgeLabel edgeLabel = tx.getEdgeLabel("rel" + RandomGenerator.randomInt(0, REL_COUNT));
            executor.execute(new SimpleReader(tx, startLatch, stopLatch, vertexId, edgeLabel.name(), EDGE_COUNT * 2, id.name()));
            startLatch.countDown();
        }
        stopLatch.await();

        propFuture.cancel(true);
        relFuture.cancel(true);
    }

    @Test
    public void concurrentIndexReadWriteTest() throws Exception {
        clopen(option(GraphDatabaseConfiguration.ADJUST_LIMIT),false);

        PropertyKey k = mgmt.makePropertyKey("k").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("q").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("byK",Vertex.class).addKey(k).buildCompositeIndex();
        finishSchema();

        final AtomicBoolean run = new AtomicBoolean(true);
        final int batchV = 10;
        final int batchR = 10;
        final int maxK = 5;
        final int maxQ = 2;
        final Random random = new Random();
        final AtomicInteger duplicates = new AtomicInteger(0);

        Thread writer = new Thread(() -> {
            while (run.get()) {
                final JanusGraphTransaction tx = graph.newTransaction();
                try {
                    for (int i = 0; i < batchV; i++) {
                        final JanusGraphVertex v = tx.addVertex();
                        v.property("k", random.nextInt(maxK));
                        v.property("q", random.nextInt(maxQ));
                    }
                    tx.commit();
                } catch (Throwable e) {
                    e.printStackTrace();
                } finally {
                    if (tx.isOpen()) tx.rollback();
                }
            }
        });
        Thread reader = new Thread(() -> {
            while (run.get()) {
                final JanusGraphTransaction tx = graph.newTransaction();
                try {
                    for (int i = 0; i < batchR; i++) {
                        final Set<Vertex> vs = new HashSet<>();
                        final Iterable<JanusGraphVertex> vertices
                                = tx.query().has("k",random.nextInt(maxK)).has("q",random.nextInt(maxQ)).vertices();
                        for (JanusGraphVertex v : vertices) {
                            if (!vs.add(v)) {
                                duplicates.incrementAndGet();
                                System.err.println("Duplicate vertex: " + v);
                            }
                        }
                    }
                    tx.commit();
                } catch (Throwable e) {
                    e.printStackTrace();
                } finally {
                    if (tx.isOpen()) tx.rollback();
                }
            }
        });
        writer.start();
        reader.start();

        Thread.sleep(10000);
        run.set(false);
        writer.join();
        reader.join();

        assertEquals(0,duplicates.get());
    }

    /**
     * Load-then-read test of standard-indexed vertex properties. This test
     * contains no edges.
     * <p>
     * The load stage is serial. The read stage is concurrent.
     * <p>
     * Create a set of vertex property types with standard indices
     * (threadPoolSize * 5 by default) serially. Serially write 1k vertices with
     * values for all of the indexed property types. Concurrently query the
     * properties. Each thread uses a single, distinct transaction for all index
     * retrievals in that thread.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testStandardIndexVertexPropertyReads() throws InterruptedException, ExecutionException {
        testStandardIndexVertexPropertyReadsLogic();
    }

    protected void testStandardIndexVertexPropertyReadsLogic() throws InterruptedException, ExecutionException {
        final int propCount = JanusGraphConcurrentTest.THREAD_COUNT * 5;
        final int vertexCount = 1000;
        // Create props with standard indexes
        log.info("Creating types");
        for (int i = 0; i < propCount; i++) {
            makeVertexIndexedUniqueKey("p"+i,String.class);
        }
        finishSchema();

        log.info("Creating vertices");
        // Write vertices with indexed properties
        for (int i = 0; i < vertexCount; i++) {
            JanusGraphVertex v = tx.addVertex();
            for (int p = 0; p < propCount; p++) {
                v.property("p" + p, i);
            }
        }
        newTx();
        log.info("Querying vertex property indices");
        // Execute runnables
        final Collection<Future<?>> futures = new ArrayList<>(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            futures.add(executor.submit(new VertexPropertyQuerier(propCount, vertexCount)));
        }
        for (Future<?> f : futures) {
            f.get();
        }
    }

    private static class RandomPropertyMaker implements Runnable {
        private final JanusGraphTransaction tx;
        private final int nodeCount; //inclusive
        private final String idKey;
        private final String randomKey;

        public RandomPropertyMaker(JanusGraphTransaction tx, int nodeCount,
                                   String idKey, String randomKey) {
            this.tx = tx;
            this.nodeCount = nodeCount;
            this.idKey = idKey;
            this.randomKey = randomKey;
        }

        @Override
        public void run() {
            while (true) {
                // Set propType to a random value on a random node
                JanusGraphVertex n = getOnlyVertex(tx.query().has(idKey, RandomGenerator.randomInt(0, nodeCount)));
                String propVal = RandomGenerator.randomString();
                n.property(randomKey, propVal);
                if (Thread.interrupted())
                    break;

                // Is creating the same property twice an error?
            }
        }
    }

    /**
     * For two nodes whose ID-property, provided at construction,
     * has the value either 0 or 1, break all existing relationships
     * from 0-node to 1-node and create a relationship of a type
     * provided at construction in the same direction.
     */
    private static class FixedRelationshipMaker implements Runnable {

        private final JanusGraphTransaction tx;
        //		private final int nodeCount; //inclusive
        private final String idKey;
        private final String edgeLabel;

        public FixedRelationshipMaker(JanusGraphTransaction tx,
                                      String id, String edgeLabel) {
            this.tx = tx;
            this.idKey = id;
            this.edgeLabel = edgeLabel;
        }

        @Override
        public void run() {
            do {
                // Make or break relType between two (possibly same) random nodes
                final JanusGraphVertex source = Iterables.getOnlyElement(tx.query().has(idKey, 0).vertices());
                final JanusGraphVertex sink = Iterables.getOnlyElement(tx.query().has(idKey, 1).vertices());
                for (Edge o : source.query().direction(Direction.OUT).labels(edgeLabel).edges()) {
                    if (getId(o.inVertex()) == getId(sink)) {
                        o.remove();
                    }
                }
                source.addEdge(edgeLabel, sink);
            } while (!Thread.interrupted());
        }

    }

    private static class SimpleReader extends BarrierRunnable {

        private final int vertexId;
        private final String label2Traverse;
        private final long nodeTraversalCount = 256;
        private final int expectedEdges;
        private final String idKey;

        public SimpleReader(JanusGraphTransaction tx, CountDownLatch startLatch,
                            CountDownLatch stopLatch, int startNodeId, String label2Traverse, int expectedEdges, String idKey) {
            super(tx, startLatch, stopLatch);
            this.vertexId = startNodeId;
            this.label2Traverse = label2Traverse;
            this.expectedEdges = expectedEdges;
            this.idKey = idKey;
        }

        @Override
        protected void doRun() {
            JanusGraphVertex v = Iterables.getOnlyElement(tx.query().has(idKey, vertexId).vertices());

            for (int i = 0; i < nodeTraversalCount; i++) {
                assertCount(expectedEdges, v.query().labels(label2Traverse).direction(Direction.BOTH).edges());
                for (JanusGraphEdge r : v.query().direction(Direction.OUT).labels(label2Traverse).edges()) {
                    v = r.vertex(Direction.IN);
                }
            }
        }
    }

    private abstract static class BarrierRunnable implements Runnable {

        protected final JanusGraphTransaction tx;
        protected final CountDownLatch startLatch;
        protected final CountDownLatch stopLatch;

        public BarrierRunnable(JanusGraphTransaction tx, CountDownLatch startLatch, CountDownLatch stopLatch) {
            this.tx = tx;
            this.startLatch = startLatch;
            this.stopLatch = stopLatch;
        }

        protected abstract void doRun();

        @Override
        public void run() {
            try {
                startLatch.await();
            } catch (Exception e) {
                throw new RuntimeException("Interrupted while waiting for peers to start");
            }

            try {
                doRun();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            stopLatch.countDown();
        }
    }

    /**
     * See {@line #testStandardIndex()}
     */
    private class VertexPropertyQuerier implements Runnable {

        private final int propCount;
        private final int vertexCount;

        public VertexPropertyQuerier(int propCount, int vertexCount) {
            this.propCount = propCount;
            this.vertexCount = vertexCount;
        }

        @Override
        public void run() {
            for (int i = 0; i < vertexCount; i++) {
                for (int p = 0; p < propCount; p++) {
                    Iterables.size(tx.query().has("p" + p, i).vertices());
                }
            }
        }
    }
}
