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

package org.janusgraph.graphdb.inmemory;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The phases of an OLAP job of {@code FulgoraGraphComputer} which run their tasks on a pool of workers after the scans,
 * the execution of the program on the partitioned vertices and the reduce stage of a map-reduce: the job waits for all
 * their tasks, as long as tasks keep ending, and fails where a task fails or none ends for
 * {@code computer.task-progress-timeout}.
 */
@Timeout(120)
public class InMemoryOlapWorkerPoolTest {

    private JanusGraph graph;

    @AfterEach
    public void closeGraph() {
        if (graph != null) {
            graph.close();
        }
    }

    private static JanusGraphFactory.Builder graphBuilder() {
        return JanusGraphFactory.build().set("storage.backend", "inmemory");
    }

    private void addVertices(int vertices) {
        final JanusGraphTransaction tx = graph.newTransaction();
        for (int i = 0; i < vertices; i++) {
            tx.addVertex();
        }
        tx.commit();
    }

    @Test
    public void aJobExecutesItsProgramOnEveryPartitionedVertex() throws Exception {
        graph = graphBuilder().set("cluster.max-partitions", 8).open();
        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("group").partition().make();
        mgmt.commit();
        final JanusGraphTransaction tx = graph.newTransaction();
        for (int i = 0; i < 12; i++) {
            tx.addVertex("group");
        }
        tx.commit();

        //On one worker, the program's second on each partitioned vertex outlasts the 10 s for which the job used to
        //wait for the vertices which the pool's queue held, before it dropped them
        final ComputerResult result = graph.compute().workers(1)
            .program(new CountingProgram(((StandardJanusGraph) graph).getIDManager())).submit().get();
        final long executed = result.memory().get(CountingProgram.EXECUTED);
        result.close();

        assertEquals(12, executed);
    }

    @Test
    public void aJobWhosePartitionedVerticesMakeNoProgressFailsWithThePoolsFailure() {
        graph = graphBuilder().set("cluster.max-partitions", 8).set("computer.task-progress-timeout", 1000).open();
        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("group").partition().make();
        mgmt.commit();
        final JanusGraphTransaction tx = graph.newTransaction();
        for (int i = 0; i < 3; i++) {
            tx.addVertex("group");
        }
        tx.commit();
        final StuckProgram program = new StuckProgram(((StandardJanusGraph) graph).getIDManager());

        try {
            final ExecutionException failure = assertThrows(ExecutionException.class,
                () -> graph.compute().workers(1).program(program).submit().get());

            //The job's failure counts the three vertices, on none of which the program ended, and is caused by the
            //pool's, which names the phase
            final Throwable job = failure.getCause().getCause();
            assertEquals("Failed to process [3] partitioned vertices in vertex program iteration [1]. Computer is " +
                "aborting.", job.getMessage());
            final String pool = job.getCause().getMessage();
            assertTrue(pool.startsWith("No task of the execution of the program on the partitioned vertices of " +
                "job ["), pool);
            assertTrue(pool.contains("ended for PT1S (computer.task-progress-timeout)"), pool);
            assertTrue(pool.contains("interrupted the 1 which ran and dropped the 2 which hadn't started"), pool);
            awaitTrue(program.interrupted);
        } finally {
            program.release.countDown();
        }
    }

    @Test
    public void aJobWaitsAsLongAsItTakesWithTheLargestProgressTimeout() throws Exception {
        //Too long for nanoseconds, which the pool saturates rather than fail on
        graph = graphBuilder().set("computer.task-progress-timeout", Long.MAX_VALUE).open();
        addVertices(10);

        final ComputerResult result = graph.compute().workers(2).mapReduce(new OrderedReduce()).submit().get();
        final long reduced = result.memory().get("reduced");
        result.close();

        assertEquals(10, reduced);
    }

    @Test
    public void aJobFailsWhereAReduceFailsAndStillEndsTheReduceStage() {
        graph = graphBuilder().open();
        addVertices(10);
        final FailingReduce mapReduce = new FailingReduce();

        final ExecutionException failure = assertThrows(ExecutionException.class,
            () -> graph.compute().workers(2).mapReduce(mapReduce).submit().get());

        assertTrue(failure.getCause() instanceof JanusGraphException, failure.toString());
        final Throwable pool = failure.getCause().getCause();
        assertTrue(pool instanceof JanusGraphException, failure.toString());
        assertTrue(pool.getMessage().startsWith("1 of the 1 tasks of the reduce stage of map-reduce [reduced] of job"),
            pool.getMessage());
        assertEquals(FailingReduce.MESSAGE, pool.getCause().getMessage());
        assertTrue(mapReduce.ended.get());
    }

    @Test
    public void aJobFailsWhereNoReduceEndsForTheProgressTimeout() {
        graph = graphBuilder().set("computer.task-progress-timeout", 1000).open();
        addVertices(10);
        final StuckReduce mapReduce = new StuckReduce();

        final long start = System.nanoTime();
        final ExecutionException failure = assertThrows(ExecutionException.class,
            () -> graph.compute().workers(2).mapReduce(mapReduce).submit().get());

        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(30));
        final String message = failure.getCause().getCause().getMessage();
        assertTrue(message.startsWith("No task of the reduce stage of map-reduce [reduced] of job"), message);
        assertTrue(message.contains("ended for PT1S (computer.task-progress-timeout)"), message);
        awaitTrue(mapReduce.interrupted);
        assertTrue(mapReduce.ended.get());
    }

    @Test
    public void aReduceStageStartsBeforeItsReducesAndEndsAfterThem() throws Exception {
        graph = graphBuilder().open();
        addVertices(100);
        final OrderedReduce mapReduce = new OrderedReduce();

        graph.compute().workers(4).mapReduce(mapReduce).submit().get().close();

        assertEquals(100, mapReduce.reduced.get());
        assertFalse(mapReduce.reducedBeforeStart.get());
        assertEquals(100, mapReduce.reducedAtEnd.get());
    }

    //Counts the vertices on which it runs, pausing for a second on each partitioned one
    private static class CountingProgram extends StaticVertexProgram<Object> {

        static final String EXECUTED = "executed";

        private final IDManager idManager;

        CountingProgram(IDManager idManager) {
            this.idManager = idManager;
        }

        @Override
        public void setup(Memory memory) {
            memory.set(EXECUTED, 0L);
        }

        @Override
        public void execute(Vertex vertex, Messenger<Object> messenger, Memory memory) {
            if (idManager.isPartitionedVertex(vertex.id())) {
                pause(1000);
            }
            memory.add(EXECUTED, 1L);
        }

        @Override
        public boolean terminate(Memory memory) {
            return true;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return Collections.singleton(MemoryComputeKey.of(EXECUTED, Operator.sumLong, false, false));
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.ORIGINAL;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }

    //Waits on each partitioned vertex until released, whatever interrupts it meanwhile
    private static class StuckProgram extends StaticVertexProgram<Object> {

        final AtomicBoolean interrupted = new AtomicBoolean();
        final CountDownLatch release = new CountDownLatch(1);
        private final IDManager idManager;

        StuckProgram(IDManager idManager) {
            this.idManager = idManager;
        }

        @Override
        public void setup(Memory memory) {
        }

        @Override
        public void execute(Vertex vertex, Messenger<Object> messenger, Memory memory) {
            while (idManager.isPartitionedVertex(vertex.id())) {
                try {
                    release.await();
                    return;
                } catch (InterruptedException e) {
                    interrupted.set(true);
                }
            }
        }

        @Override
        public boolean terminate(Memory memory) {
            return true;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return Collections.emptySet();
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.ORIGINAL;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }

    //Maps every vertex to one key, or each to a key of its own, and reduces it as the subclass does
    private abstract static class TestReduce extends StaticMapReduce<Long, Long, Long, Long, Long> {

        final AtomicBoolean ended = new AtomicBoolean();
        private final boolean keyPerVertex;
        private final AtomicLong keys = new AtomicLong();

        TestReduce(boolean keyPerVertex) {
            this.keyPerVertex = keyPerVertex;
        }

        @Override
        public boolean doStage(Stage stage) {
            return true;
        }

        @Override
        public void map(Vertex vertex, MapEmitter<Long, Long> emitter) {
            emitter.emit(keyPerVertex ? keys.incrementAndGet() : 1L, 1L);
        }

        @Override
        public void workerEnd(Stage stage) {
            if (stage == Stage.REDUCE) {
                ended.set(true);
            }
        }

        @Override
        public Long generateFinalResult(Iterator<KeyValue<Long, Long>> keyValues) {
            long reduced = 0;
            while (keyValues.hasNext()) {
                reduced += keyValues.next().getValue();
            }
            return reduced;
        }

        @Override
        public String getMemoryKey() {
            return "reduced";
        }
    }

    private static class FailingReduce extends TestReduce {

        static final String MESSAGE = "The reduce failed";

        FailingReduce() {
            super(false);
        }

        @Override
        public void reduce(Long key, Iterator<Long> values, ReduceEmitter<Long, Long> emitter) {
            throw new IllegalStateException(MESSAGE);
        }
    }

    //A reduce which never ends, unless interrupted
    private static class StuckReduce extends TestReduce {

        final AtomicBoolean interrupted = new AtomicBoolean();
        private final CountDownLatch never = new CountDownLatch(1);

        StuckReduce() {
            super(false);
        }

        @Override
        public void reduce(Long key, Iterator<Long> values, ReduceEmitter<Long, Long> emitter) {
            try {
                never.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        }
    }

    //Records whether reduces run before the stage starts, and how many ended before it ends
    private static class OrderedReduce extends TestReduce {

        final AtomicInteger reduced = new AtomicInteger();
        final AtomicBoolean reducedBeforeStart = new AtomicBoolean();
        final AtomicInteger reducedAtEnd = new AtomicInteger(-1);
        private final AtomicBoolean started = new AtomicBoolean();

        OrderedReduce() {
            super(true);
        }

        @Override
        public void workerStart(Stage stage) {
            if (stage == Stage.REDUCE) {
                pause(100);
                started.set(true);
            }
        }

        @Override
        public void reduce(Long key, Iterator<Long> values, ReduceEmitter<Long, Long> emitter) {
            if (!started.get()) {
                reducedBeforeStart.set(true);
            }
            pause(10);
            reduced.incrementAndGet();
            emitter.emit(key, 1L);
        }

        @Override
        public void workerEnd(Stage stage) {
            if (stage == Stage.REDUCE) {
                reducedAtEnd.set(reduced.get());
            }
            super.workerEnd(stage);
        }
    }

    private static void awaitTrue(AtomicBoolean flag) {
        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!flag.get() && System.nanoTime() < end) {
            pause(10);
        }
        assertTrue(flag.get());
    }

    //Pauses for the time whatever interrupts the thread meanwhile, as work which doesn't heed interrupts does
    private static void pause(long millis) {
        final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        for (long left = millis; left > 0; left = TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime())) {
            try {
                Thread.sleep(left);
            } catch (InterruptedException e) {
                //Goes on pausing
            }
        }
    }
}
