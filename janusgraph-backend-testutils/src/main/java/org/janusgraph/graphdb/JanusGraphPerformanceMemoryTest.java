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

import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.TestCategory;
import org.janusgraph.testutil.MemoryAssess;
/**
 * These tests focus on the in-memory data structures of individual transactions and how they hold up to high memory pressure
 */
@Tag(TestCategory.MEMORY_TESTS)
public abstract class JanusGraphPerformanceMemoryTest extends JanusGraphBaseTest {

    @RepeatedTest(10)
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
                JanusGraphTransaction tx = graph.newTransaction();
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

    @RepeatedTest(10)
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
            writeThreads[t] = new Thread(() -> {
                for (int r = 0; r < rounds; r++) {
                    JanusGraphTransaction tx = graph.newTransaction();
                    JanusGraphVertex previous = null;
                    for (int c = 0; c < commitSize; c++) {
                        JanusGraphVertex v = tx.addVertex();
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
            });
            writeThreads[t].start();
        }
        for (final Thread writeThread : writeThreads) {
            writeThread.join();
        }
        System.out.println("Write time for " + (rounds * commitSize * writeThreads.length) + " vertices & edges: " + (System.currentTimeMillis() - start));

        final int maxUID = uidCounter.get();
        final int trials = 1000;
        final String fixedName = "john";
        Thread[] readThreads = new Thread[Runtime.getRuntime().availableProcessors() * 2];
        start = System.currentTimeMillis();
        for (int t = 0; t < readThreads.length; t++) {
            readThreads[t] = new Thread(() -> {
                JanusGraphTransaction tx = graph.newTransaction();
                long randomUniqueId = random.nextInt(maxUID) + 1;
                getVertex(tx,"uid", randomUniqueId).property(VertexProperty.Cardinality.single, "name",  fixedName);
                for (int t1 = 1; t1 <= trials; t1++) {
                    JanusGraphVertex v = getVertex(tx,"uid", random.nextInt(maxUID) + 1);
                    assertCount(2, v.properties());
                    int count = 0;
                    for (Object e : v.query().direction(Direction.BOTH).edges()) {
                        count++;
                        assertTrue(((JanusGraphEdge) e).<Integer>value("time") >= 0);
                    }
                    assertTrue(count <= 2);
//                        if (t%(trials/10)==0) System.out.println(t);

                }
                assertEquals(getVertex(tx,"uid", randomUniqueId).value("name"), fixedName);
                tx.commit();
            });
            readThreads[t].start();
        }
        for (final Thread readThread : readThreads) {
            readThread.join();
        }
        System.out.println("Read time for " + (trials * readThreads.length) + " vertex lookups: " + (System.currentTimeMillis() - start));

    }
}


