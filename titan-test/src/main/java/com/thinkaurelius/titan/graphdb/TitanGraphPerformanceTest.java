package com.thinkaurelius.titan.graphdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.NumberFormat;
import java.util.LinkedHashMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.StandardEdge;
import com.thinkaurelius.titan.testutil.JUnitBenchmarkProvider;
import com.thinkaurelius.titan.testutil.MemoryAssess;
import com.thinkaurelius.titan.testutil.PerformanceTest;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

@BenchmarkOptions(warmupRounds = 0, benchmarkRounds = 3)
public abstract class TitanGraphPerformanceTest extends TitanGraphTestCommon {

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();

    private static final int minBatchSizeAsPowerOf2 = 4;
    private static final int maxBatchSizeAsPowerOf2 = 14;
    private final int trials;
    private final int jitPretrials;
    private final boolean tryBatching;

    private final LinkedHashMap<String, Metric> metrics = new LinkedHashMap<String, Metric>();

    public TitanGraphPerformanceTest(Configuration config) {
        this(config, 0, 1, false);
    }

    public TitanGraphPerformanceTest(Configuration config, int jitPretrials, int trials, boolean tryBatching) {
        super(config);
        this.jitPretrials = jitPretrials;
        this.trials = trials;
        this.tryBatching = tryBatching;
    }

    @Test
    public void testMultipleDatabases() {
        long memoryBaseline = 0;
        for (int i = 0; i < 100; i++) {
            graph.addVertex(null);
            clopen();
            if (i == 1) {
                memoryBaseline = MemoryAssess.getMemoryUse();
                System.out.println("Memory before: " + memoryBaseline / 1024);
            }
        }
        close();
        long memoryAfter = MemoryAssess.getMemoryUse();
        System.out.println("Memory after: " + memoryAfter / 1024);
        //assertTrue(memoryAfter<100*1024*1024);
    }

    @Test
    public void vertexCreation() {
        TitanLabel connect = makeSimpleEdgeLabel("connect");
        int noNodes = 20000;
        TitanVertex[] nodes = new TitanVertex[noNodes];
        PerformanceTest p = new PerformanceTest(true);
        for (int i = 0; i < noNodes; i++) {
            nodes[i] =
                    tx.addVertex();
        }
        p.end();
        System.out.println("Time per node in (ns): " + (p.getNanoTime() / noNodes));

        p = new PerformanceTest(true);
        for (int i = 0; i < noNodes; i++) {
            new StandardEdge(i + 1, connect, (InternalVertex) nodes[i], (InternalVertex) nodes[(i + 1) % noNodes], ElementLifeCycle.New);
        }
        p.end();
        System.out.println("Time per edge in (ns): " + (p.getNanoTime() / noNodes));

        p = new PerformanceTest(true);
        for (int i = 0; i < noNodes; i++) {
            nodes[i].addEdge(connect, nodes[(i + 1) % noNodes]);
        }
        p.end();
        System.out.println("Time per edge creation+connection in (ns): " + (p.getNanoTime() / noNodes));
        tx.rollback();
        tx = null;
    }

    @Test
    public void testInTxIndex() throws Exception {
        int trials = 2;
        int numV = 2000;
        int offset = 10000;
        tx.makeType().name("uid").dataType(Long.class).indexed(Vertex.class).vertexUnique(Direction.OUT).makePropertyKey();
        newTx();

        long start = System.currentTimeMillis();
        for (int t = 0; t < trials; t++) {
            for (int i = offset; i < offset + numV; i++) {
                if (Iterables.isEmpty(tx.getVertices("uid", Long.valueOf(i)))) {
                    Vertex v = tx.addVertex();
                    v.setProperty("uid", Long.valueOf(i));
                }
            }
        }
        assertEquals(numV, Iterables.size(tx.getVertices()));
        System.out.println("Total time (ms): " + (System.currentTimeMillis() - start));
    }

    @Test
    public void unlabeledEdgeInsertion() throws Exception {
        runEdgeInsertion(new UnlabeledEdgeInsertion());
    }

    @Test
    public void labeledEdgeInsertion() throws Exception {
        runEdgeInsertion(new LabeledEdgeInsertion());
    }

    private void runEdgeInsertion(Runnable task) throws Exception {

        if (tryBatching)
            for (int pow = minBatchSizeAsPowerOf2; pow <= maxBatchSizeAsPowerOf2; pow += 1)
                performanceTest(task, true, 1 << pow);

        performanceTest(task, false, 0);
    }

    private void performanceTest(Runnable task, boolean batching, int batchSize) throws Exception {

        String batchStatus;
        if (batching)
            batchStatus = "batching=true; batchSize=" + batchSize;
        else
            batchStatus = "batching=false";

        System.out.println("Beginning " + trials + " trials (" + jitPretrials + " pretrials) of [" + task + "]; " + batchStatus);

        for (int trial = 0; trial < trials + jitPretrials; trial++) {

            if (jitPretrials == trial)
                metrics.clear();

            if (null != config) {
                close();
                config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BATCH_KEY, true);
                open();
            }

            if (trial < jitPretrials)
                System.out.println("Starting pretrial " + (trial + 1) + "/" + jitPretrials);
            else
                System.out.println("Starting trial " + (trial - jitPretrials + 1) + "/" + trials);

            task.run();

            // Clean database
            tearDown();
            setUp();

            // Give the VM a garbage collection timing hint
            System.gc();
            Thread.sleep(100L);
        }

        logMetrics();
        System.out.println("Beginning " + trials + " trials (" + jitPretrials + " pretrials) of [" + task + "]; " + batchStatus);
    }

    private Metric getMetric(String description, String units) {
        String key = description;
        Metric m = metrics.get(key);
        if (null == m) {
            m = new Metric(description, units);
            metrics.put(key, m);
        }
        return m;
    }

    private void logMetrics() {
        for (Metric m : metrics.values())
            System.out.println(m.toString());
    }

    private abstract class EdgeInsertion implements Runnable {

        protected final int noNodes;
        protected final int noEdgesPerNode;

        protected EdgeInsertion() {
            this(50000, 10);
        }

        protected EdgeInsertion(int noNodes, int noEdgesPerNode) {
            this.noNodes = noNodes;
            this.noEdgesPerNode = noEdgesPerNode;
        }

        public void run() {

            final long start = System.nanoTime();
            final long mil = 1000000;

            doLoad();

            long precommit = System.nanoTime();
            tx.commit();
            long postcommit = System.nanoTime();
            clopen();
            long end = System.nanoTime();

            long precommitMS = (precommit - start) / mil;
            long postcommitMS = (postcommit - start) / mil;
            long endMS = (end - start) / mil;

            getMetric("Time from start to just before commit", "ms").addValue(
                    precommitMS);
            getMetric("Time from start through commit", "ms").addValue(
                    postcommitMS);
            getMetric("Total time", "ms").addValue(endMS);

            long edgesPerSec = noNodes * noEdgesPerNode * 1000
                    / Math.max(1, postcommitMS - precommitMS);
            getMetric("TitanRelation commit rate", "edges/sec").addValue(edgesPerSec);

            //Verify that data was written
            TitanVertex v1 = (TitanVertex) Iterables.getOnlyElement(tx.getVertices("uid", 50));
            TitanVertex v2 = (TitanVertex) Iterables.getOnlyElement(tx.getVertices("uid", 150));
            assertTrue(v1.query().count() > 0);
            assertEquals(v1.query().count(), v2.query().count());
        }

        protected abstract void doLoad();
    }

    private class LabeledEdgeInsertion extends EdgeInsertion {

        private LabeledEdgeInsertion() {
            super();
        }

        @Override
        public String toString() {
            return "labeled edge insertion";
        }

        @Override
        protected void doLoad() {
            TitanKey weight = tx.makeType().name("weight").
                    vertexUnique(Direction.OUT).
                    dataType(Double.class).
                    makePropertyKey();
            TitanKey id = tx.makeType().name("uid").
                    vertexUnique(Direction.OUT).
                    //unique(Direction.IN).
                            indexed(Vertex.class).
                    dataType(Integer.class).
                    makePropertyKey();
            TitanLabel knows = tx.makeType().name("knows").
                    primaryKey(id).signature(weight).directed().makeEdgeLabel();
            TitanKey name = tx.makeType().name("name").vertexUnique(Direction.OUT)
                    .indexed(Vertex.class).dataType(String.class).makePropertyKey();

            String[] names = new String[noNodes];
            TitanVertex[] nodes = new TitanVertex[noNodes];
            for (int i = 0; i < noNodes; i++) {
                names[i] = "Node" + i;
                nodes[i] = tx.addVertex();
                nodes[i].addProperty(name, names[i]);
                nodes[i].addProperty(id, i);
                if ((i + 1) % 100 == 0) System.out.println("" + (i + 1));
            }
            System.out.println("Nodes loaded.");
            int offsets[] = {-99, -71, -20, -17, -13, 2, 7, 15, 33, 89};
            assert offsets.length == noEdgesPerNode;

            for (int i = 0; i < noNodes; i++) {
                TitanVertex n = nodes[i];
                for (int e = 0; e < noEdgesPerNode; e++) {
                    TitanVertex n2 = nodes[wrapAround(i + offsets[e], noNodes)];
                    TitanEdge r = n.addEdge(knows, n2);
                    r.setProperty(id, RandomGenerator.randomInt(0, Integer.MAX_VALUE));
                    r.setProperty(weight, Math.random());
                }
                if ((i + 1) % 10000 == 0) System.out.println("" + (i + 1));
            }
        }
    }

    private class UnlabeledEdgeInsertion extends EdgeInsertion {

        public UnlabeledEdgeInsertion() {
            super();
        }

        @Override
        public String toString() {
            return "unlabeled edge insertion";
        }

        @Override
        public void doLoad() {

            TitanLabel connect = makeSimpleEdgeLabel("connect");
            TitanKey name = makeUniqueStringPropertyKey("name");
            TitanKey id = makeIntegerUIDPropertyKey("uid");

            String[] names = new String[noNodes];
            TitanVertex[] nodes = new TitanVertex[noNodes];
            for (int i = 0; i < noNodes; i++) {
                names[i] = "Node" + i;
                nodes[i] = tx.addVertex();
                nodes[i].addProperty(name, names[i]);
                nodes[i].addProperty(id, i);
            }
            System.out.println("Nodes loaded.");
            int offsets[] = {-99, -71, -20, -17, -13, 2, 7, 15, 33, 89};
            assert offsets.length == noEdgesPerNode;

            for (int i = 0; i < noNodes; i++) {
                TitanVertex n = nodes[i];
                for (int e = 0; e < noEdgesPerNode; e++) {
                    TitanVertex n2 = nodes[wrapAround(i + offsets[e], noNodes)];
                    n.addEdge(connect, n2);
                }
                if ((i + 1) % 10000 == 0)
                    System.out.println("" + (i + 1));
            }
        }
    }


    private static class Metric {
        private final String description;
        private final String units;
        private final DescriptiveStatistics stats;

        Metric(String description, String units) {
            this.description = description;
            this.units = units;
            this.stats = new DescriptiveStatistics();
        }

        void addValue(double d) {
            stats.addValue(d);
        }

        @Override
        public String toString() {
            NumberFormat fmt = NumberFormat.getNumberInstance();
            fmt.setMaximumFractionDigits(1);

            return description + ":" +
                    " avg=" + fmt.format(stats.getMean()) +
                    " stdev=" + fmt.format(stats.getStandardDeviation()) +
                    " " + units +
                    " (" + stats.getN() + " samples)";
        }
    }

}


