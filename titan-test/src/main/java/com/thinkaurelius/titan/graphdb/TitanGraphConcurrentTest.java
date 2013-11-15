package com.thinkaurelius.titan.graphdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.core.*;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.testcategory.PerformanceTests;
import com.thinkaurelius.titan.testutil.JUnitBenchmarkProvider;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

@Category({PerformanceTests.class})
public abstract class TitanGraphConcurrentTest extends TitanGraphTestCommon {

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();

    // Parallelism settings
    private static final int THREAD_COUNT = getThreadCount();
    private static final int TASK_COUNT = THREAD_COUNT * 256;

    // Graph structure settings
    private static final int NODE_COUNT = 1000;
    private static final int EDGE_COUNT = 5;
    private static final int REL_COUNT = 5;

    private static final Logger log =
            LoggerFactory.getLogger(TitanGraphConcurrentTest.class);

    private ExecutorService executor;

    public TitanGraphConcurrentTest(Configuration config) {
        super(config);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        executor = Executors.newFixedThreadPool(THREAD_COUNT);
        // Generate synthetic graph

        TitanLabel[] rels = new TitanLabel[REL_COUNT];
        for (int i = 0; i < rels.length; i++) {
            rels[i] = makeSimpleEdgeLabel("rel" + i);
        }
        TitanKey id = makeIntegerUIDPropertyKey("uid");
        TitanVertex nodes[] = new TitanVertex[NODE_COUNT];
        for (int i = 0; i < NODE_COUNT; i++) {
            nodes[i] = tx.addVertex();
            nodes[i].addProperty(id, i);
        }
        for (int i = 0; i < NODE_COUNT; i++) {
            for (int r = 0; r < rels.length; r++) {
                for (int j = 1; j <= EDGE_COUNT; j++) {
                    nodes[i].addEdge(rels[r], nodes[wrapAround(i + j, NODE_COUNT)]);
                }
            }
        }

        // Get a new transaction
        clopen();
    }

    @Override
    @After
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
            KeyMaker tm = tx.makeKey("test" + i).dataType(String.class).single();
            if (i % 4 == 0) tm.unique().indexed(Vertex.class);

            tm.make();
        }
        for (int i = numTypes / 2; i < numTypes; i++) {
            LabelMaker tm = tx.makeLabel("test" + i);
            if (i % 4 == 1) tm.unidirected();
            tm.make();
        }
        clopen();
        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            threads[t] = new Thread(new Runnable() {
                @Override
                public void run() {
                    TitanTransaction tx = graph.newTransaction();
                    for (int i = 0; i < numTypes; i++) {
                        TitanType type = tx.getType("test" + i);
                        if (i < numTypes / 2) assertTrue(type.isPropertyKey());
                        else assertTrue(type.isEdgeLabel());
                    }
                    tx.commit();
                }
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
        TitanKey id = tx.getPropertyKey("uid");

        // Tail many concurrent readers on a single transaction
        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            int nodeid = RandomGenerator.randomInt(0, NODE_COUNT);
            TitanLabel rel = tx.getEdgeLabel("rel" + RandomGenerator.randomInt(0, REL_COUNT));
            executor.execute(new SimpleReader(tx, startLatch, stopLatch, nodeid, rel, EDGE_COUNT * 2, id));
            startLatch.countDown();
        }
        stopLatch.await();
    }

    /**
     * Tail many readers, as in {@link #concurrentReadsOnSingleTransaction()},
     * but also start some threads that add and remove relationships and
     * properties while the readers are working; all tasks share a common
     * transaction.
     * <p/>
     * The readers do not look for the properties or relationships the
     * writers are mutating, since this is all happening on a common transaction.
     *
     * @throws Exception
     */
    @Test
    public void concurrentReadWriteOnSingleTransaction() throws Exception {
        TitanKey id = tx.getPropertyKey("uid");

        Runnable propMaker =
                new RandomPropertyMaker(tx, NODE_COUNT, id,
                        makeUniqueStringPropertyKey("dummyProperty"));
        Runnable relMaker =
                new FixedRelationshipMaker(tx, id,
                        makeSimpleEdgeLabel("dummyRelationship"));

        newTx();

        Future<?> propFuture = executor.submit(propMaker);
        Future<?> relFuture = executor.submit(relMaker);

        CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
        CountDownLatch stopLatch = new CountDownLatch(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            int nodeid = RandomGenerator.randomInt(0, NODE_COUNT);
            TitanLabel rel = tx.getEdgeLabel("rel" + RandomGenerator.randomInt(0, REL_COUNT));
            executor.execute(new SimpleReader(tx, startLatch, stopLatch, nodeid, rel, EDGE_COUNT * 2, id));
            startLatch.countDown();
        }
        stopLatch.await();

        propFuture.cancel(true);
        relFuture.cancel(true);
    }

    /**
     * Load-then-read test of standard-indexed vertex properties. This test
     * contains no edges.
     * <p/>
     * The load stage is serial. The read stage is concurrent.
     * <p/>
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
        final int propCount = THREAD_COUNT * 5;
        final int vertexCount = 1 * 1000;
        // Create props with standard indexes
        log.info("Creating types");
        for (int i = 0; i < propCount; i++) {
            tx.makeKey("p" + i).dataType(Integer.class)
                    .indexed(Vertex.class).single().unique().make();
        }
        newTx();
        log.info("Creating vertices");
        // Write vertices with indexed properties
        for (int i = 0; i < vertexCount; i++) {
            TitanVertex v = tx.addVertex();
            for (int p = 0; p < propCount; p++) {
                tx.addProperty(v, "p" + p, i);
            }
        }
        newTx();
        log.info("Querying vertex property indices");
        // Execute runnables
        Collection<Future<?>> futures = new ArrayList<Future<?>>(TASK_COUNT);
        for (int i = 0; i < TASK_COUNT; i++) {
            futures.add(executor.submit(new VertexPropertyQuerier(propCount, vertexCount)));
        }
        for (Future<?> f : futures) {
            f.get();
        }
    }

    private static class RandomPropertyMaker implements Runnable {
        private final TitanTransaction tx;
        private final int nodeCount; //inclusive
        private final TitanKey idProp;
        private final TitanKey randomProp;

        public RandomPropertyMaker(TitanTransaction tx, int nodeCount,
                                   TitanKey idProp, TitanKey randomProp) {
            this.tx = tx;
            this.nodeCount = nodeCount;
            this.idProp = idProp;
            this.randomProp = randomProp;
        }

        @Override
        public void run() {
            while (true) {
                // Set propType to a random value on a random node
                TitanVertex n = Iterables.getOnlyElement(tx.getVertices(idProp, RandomGenerator.randomInt(0, nodeCount)));
                String propVal = RandomGenerator.randomString();
                n.addProperty(randomProp, propVal);
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

        private final TitanTransaction tx;
        //		private final int nodeCount; //inclusive
        private final TitanKey idProp;
        private final TitanLabel relType;

        public FixedRelationshipMaker(TitanTransaction tx,
                                      TitanKey id, TitanLabel relType) {
            this.tx = tx;
            this.idProp = id;
            this.relType = relType;
        }

        @Override
        public void run() {
            while (true) {
                // Make or break relType between two (possibly same) random nodes
//				TitanVertex source = tx.getVertex(idProp, RandomGenerator.randomInt(0, nodeCount));
//				TitanVertex sink = tx.getVertex(idProp, RandomGenerator.randomInt(0, nodeCount));
                TitanVertex source = Iterables.getOnlyElement(tx.getVertices(idProp, 0));
                TitanVertex sink = Iterables.getOnlyElement(tx.getVertices(idProp, 1));
                for (TitanEdge r : source.getTitanEdges(Direction.OUT, relType)) {
                    if (r.getVertex(Direction.IN).getID() == sink.getID()) {
                        r.remove();
                        continue;
                    }
                }
                source.addEdge(relType, sink);
                if (Thread.interrupted())
                    break;
            }
        }

    }

    private static class SimpleReader extends BarrierRunnable {

        private final int nodeid;
        private final TitanLabel relTypeToTraverse;
        private final long nodeTraversalCount = 256;
        private final int expectedEdges;
        private final TitanKey id;

        public SimpleReader(TitanTransaction tx, CountDownLatch startLatch,
                            CountDownLatch stopLatch, int startNodeId, TitanLabel relTypeToTraverse, int expectedEdges, TitanKey id) {
            super(tx, startLatch, stopLatch);
            this.nodeid = startNodeId;
            this.relTypeToTraverse = relTypeToTraverse;
            this.expectedEdges = expectedEdges;
            this.id = id;
        }

        @Override
        protected void doRun() throws Exception {
            TitanVertex n = Iterables.getOnlyElement(tx.getVertices(id, nodeid));

            for (int i = 0; i < nodeTraversalCount; i++) {
                assertEquals("On vertex: " + n.getID(), expectedEdges, Iterables.size(n.getTitanEdges(Direction.BOTH, relTypeToTraverse)));
                for (TitanEdge r : n.getTitanEdges(Direction.OUT, relTypeToTraverse)) {
                    n = r.getVertex(Direction.IN);
                }
            }
        }
    }

    private abstract static class BarrierRunnable implements Runnable {

        protected final TitanTransaction tx;
        protected final CountDownLatch startLatch;
        protected final CountDownLatch stopLatch;

        public BarrierRunnable(TitanTransaction tx, CountDownLatch startLatch, CountDownLatch stopLatch) {
            this.tx = tx;
            this.startLatch = startLatch;
            this.stopLatch = stopLatch;
        }

        protected abstract void doRun() throws Exception;

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
                    tx.getVertices("p" + p, i);
                }
            }
        }
    }
}
