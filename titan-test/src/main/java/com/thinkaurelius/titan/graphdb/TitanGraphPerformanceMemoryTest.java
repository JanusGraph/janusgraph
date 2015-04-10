package com.thinkaurelius.titan.graphdb;

import static com.thinkaurelius.titan.testutil.TitanAssert.assertCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.testcategory.MemoryTests;
import com.thinkaurelius.titan.testutil.JUnitBenchmarkProvider;
import com.thinkaurelius.titan.testutil.MemoryAssess;

/**
 * These tests focus on the in-memory data structures of individual transactions and how they hold up to high memory pressure
 */
@Category({ MemoryTests.class })
public abstract class TitanGraphPerformanceMemoryTest extends TitanGraphBaseTest {

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();

    @Test
    public void testMemoryLeakage() {
        long memoryBaseline = 0;
        SummaryStatistics stats = new SummaryStatistics();
        int numRuns = 25;
        for (int r = 0; r < numRuns; r++) {
            if (r == 1 || r == (numRuns - 1)) {
                memoryBaseline = MemoryAssess.getMemoryUse();
                stats.addValue(memoryBaseline);
                //System.out.println("Memory before run "+(r+1)+": " + memoryBaseline / 1024 + " KB");
            }
            for (int t = 0; t < 1000; t++) {
                graph.addVertex();
                graph.tx().rollback();
                TitanTransaction tx = graph.newTransaction();
                tx.addVertex();
                tx.rollback();
            }
            if (r == 1 || r == (numRuns - 1)) {
                memoryBaseline = MemoryAssess.getMemoryUse();
                stats.addValue(memoryBaseline);
                //System.out.println("Memory after run " + (r + 1) + ": " + memoryBaseline / 1024 + " KB");
            }
            clopen();
        }
        System.out.println("Average: " + stats.getMean() + " Std. Dev: " + stats.getStandardDeviation());
        assertTrue(stats.getStandardDeviation() < stats.getMin());
    }

    @Test
    public void testTransactionalMemory() throws Exception {
        makeVertexIndexedUniqueKey("uid",Long.class);
        makeKey("name",String.class);

        PropertyKey time = makeKey("time",Integer.class);
        mgmt.makeEdgeLabel("friend").signature(time).directed().make();
        finishSchema();

        final Random random = new Random();
        final int rounds = 100;
        final int commitSize = 1500;
        final AtomicInteger uidCounter = new AtomicInteger(0);
        Thread[] writeThreads = new Thread[4];
        long start = System.currentTimeMillis();
        for (int t = 0; t < writeThreads.length; t++) {
            writeThreads[t] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int r = 0; r < rounds; r++) {
                        TitanTransaction tx = graph.newTransaction();
                        TitanVertex previous = null;
                        for (int c = 0; c < commitSize; c++) {
                            TitanVertex v = tx.addVertex();
                            long uid = uidCounter.incrementAndGet();
                            v.property(VertexProperty.Cardinality.single, "uid",  uid);
                            v.property(VertexProperty.Cardinality.single, "name",  "user" + uid);
                            if (previous != null) {
                                v.addEdge("friend", previous, "time", Math.abs(random.nextInt()));
                            }
                            previous = v;
                        }
                        tx.commit();
                    }
                }
            });
            writeThreads[t].start();
        }
        for (int t = 0; t < writeThreads.length; t++) {
            writeThreads[t].join();
        }
        System.out.println("Write time for " + (rounds * commitSize * writeThreads.length) + " vertices & edges: " + (System.currentTimeMillis() - start));

        final int maxUID = uidCounter.get();
        final int trials = 1000;
        final String fixedName = "john";
        Thread[] readThreads = new Thread[Runtime.getRuntime().availableProcessors() * 2];
        start = System.currentTimeMillis();
        for (int t = 0; t < readThreads.length; t++) {
            readThreads[t] = new Thread(new Runnable() {
                @Override
                public void run() {
                    TitanTransaction tx = graph.newTransaction();
                    long ruid = random.nextInt(maxUID) + 1;
                    getVertex(tx,"uid", ruid).property(VertexProperty.Cardinality.single, "name",  fixedName);
                    for (int t = 1; t <= trials; t++) {
                        TitanVertex v = getVertex(tx,"uid", random.nextInt(maxUID) + 1);
                        assertCount(2, v.properties());
                        int count = 0;
                        for (TitanEdge e : v.query().direction(Direction.BOTH).edges()) {
                            count++;
                            assertTrue(e.<Integer>value("time") >= 0);
                        }
                        assertTrue(count <= 2);
//                        if (t%(trials/10)==0) System.out.println(t);
                    }
                    assertEquals(fixedName, getVertex(tx,"uid", ruid).value("name"));
                    tx.commit();
                }
            });
            readThreads[t].start();
        }
        for (int t = 0; t < readThreads.length; t++) {
            readThreads[t].join();
        }
        System.out.println("Read time for " + (trials * readThreads.length) + " vertex lookups: " + (System.currentTimeMillis() - start));

    }
}


