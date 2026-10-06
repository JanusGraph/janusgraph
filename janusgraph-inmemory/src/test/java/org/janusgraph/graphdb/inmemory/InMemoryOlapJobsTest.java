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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.GraphIndexStatusReport;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.janusgraph.olap.OLAPTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The transactions and schema changes of the OLAP jobs of {@code FulgoraGraphComputer}: the elements a job's scans read
 * stay usable until the job ends, the job leaves no transaction open after that, so that the graph goes on
 * acknowledging schema changes, and jobs which run at once create a missing compute key once.
 */
public class InMemoryOlapJobsTest {

    private static final int VERTICES = 2000;

    private JanusGraph graph;

    @AfterEach
    public void closeGraph() {
        if (graph != null) {
            graph.close();
        }
    }

    private static JanusGraphFactory.Builder graphBuilder() {
        return JanusGraphFactory.build()
            .set("storage.backend", "inmemory")
            //Work blocks of 200 vertices: the scanner's processors take at least ten clones of a job through the
            //2,000 vertices, each with a transaction of its own
            .set("storage.buffer-size", 20)
            //The management log is read every tenth of a second rather than every five seconds
            .set("log.janusgraph.read-interval", 100);
    }

    //A chain of vertices named v0, v1, ..., each with an edge to the one before
    private static JanusGraph openGraph(JanusGraphFactory.Builder builder, int vertices) {
        final JanusGraph graph = builder.open();
        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makeEdgeLabel("knows").make();
        mgmt.commit();
        final JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex previous = null;
        for (int i = 0; i < vertices; i++) {
            final JanusGraphVertex vertex = tx.addVertex();
            vertex.property("name", "v" + i);
            if (previous != null) {
                vertex.addEdge("knows", previous);
            }
            previous = vertex;
        }
        tx.commit();
        return graph;
    }

    private Set<JanusGraphTransaction> openTransactions() {
        return new HashSet<>(((StandardJanusGraph) graph).getOpenTransactions());
    }

    @Test
    public void anOlapJobLeavesNoTransactionOpen() throws Exception {
        graph = openGraph(graphBuilder(), VERTICES);
        final Set<JanusGraphTransaction> before = openTransactions();

        //A vertex program and a map job, which scan on four processors
        final ComputerResult result = graph.compute().workers(4).program(new OLAPTest.DegreeCounter())
            .mapReduce(new OLAPTest.DegreeMapper()).submit().get();
        result.close();

        assertEquals(before, openTransactions());
    }

    @Test
    public void theElementsWhichAScanReadStayUsableUntilTheJobEnds() {
        graph = openGraph(graphBuilder(), VERTICES);
        final Set<JanusGraphTransaction> before = openTransactions();

        //The order step collects the vertices in the memory, which the last iteration's terminate() detaches, and the
        //job attaches them to the graph again in a transaction of its thread
        final List<Vertex> vertices = graph.traversal().withComputer().V().order().by("name").toList();

        assertEquals(IntStream.range(0, VERTICES).mapToObj(i -> "v" + i).sorted().collect(Collectors.toList()),
            vertices.stream().map(vertex -> vertex.<String>value("name")).collect(Collectors.toList()));
        //The traversal attaches the results to this thread's transaction, which is the caller's to end
        graph.tx().rollback();
        assertEquals(before, openTransactions());
    }

    @Test
    public void theEdgesWhichAnIterationReadStayUsableInTheNextOne() {
        //Work blocks of 10 vertices, so that the iterations' scans take several clones through the 30
        graph = openGraph(graphBuilder().set("storage.buffer-size", 1), 30);

        //Each iteration extends the paths with the edges which the iteration before read
        final List<Path> paths = graph.traversal().withComputer().V().has("name", "v29").shortestPath()
            .with(ShortestPath.target, __.has("name", "v0"))
            .with(ShortestPath.includeEdges, true).toList();

        assertEquals(1, paths.size());
        assertEquals(59, paths.get(0).size());
        assertTrue(paths.get(0).<Object>get(1) instanceof Edge);
        graph.tx().rollback();
    }

    @Test
    public void theElementsInAJobsMemoryStayUsableAfterTheJob() throws Exception {
        graph = openGraph(graphBuilder(), VERTICES);
        final Set<JanusGraphTransaction> before = openTransactions();

        final ComputerResult result = graph.compute().workers(4).program(new CollectingProgram(false)).submit().get();
        final List<Vertex> vertices = result.memory().get(CollectingProgram.VERTICES_KEY);
        result.close();

        assertEquals(before, openTransactions());
        //Detached from the scans' transactions, with their properties
        assertEquals(IntStream.range(0, VERTICES).mapToObj(i -> "v" + i).sorted().collect(Collectors.toList()),
            vertices.stream().map(vertex -> vertex.<String>value("name")).sorted().collect(Collectors.toList()));
    }

    @Test
    public void aJobWhoseMemoryHoldsAdjacentVerticesEndsAndKeepsTheirIds() throws Exception {
        graph = openGraph(graphBuilder(), VERTICES);
        final Set<Object> knownIds = graph.traversal().V().out("knows").id().toSet();
        final Set<JanusGraphTransaction> before = openTransactions();

        //Neither the label nor the properties of a vertex adjacent to a scanned one can be read in an OLAP job, so
        //such a value can't be detached and stays as it is, its ids readable
        final ComputerResult result = graph.compute().workers(4).program(new CollectingProgram(true)).submit().get();
        final List<Vertex> adjacent = result.memory().get(CollectingProgram.VERTICES_KEY);
        result.close();

        assertEquals(before, openTransactions());
        assertEquals(knownIds, adjacent.stream().map(Vertex::id).collect(Collectors.toSet()));
    }

    @Test
    public void aJobOfATraversalReadsTheElementsWhichTheJobBeforeCollected() {
        graph = openGraph(graphBuilder(), VERTICES);
        final Set<Object> knownIds = graph.traversal().V().out("knows").id().toSet();
        assertEquals(VERTICES - 1, knownIds.size());

        //The first job collects the vertices which it reaches through an edge, attached to its scan's transactions,
        //and detaches them when it ends; the third job, the traversal of cap("m"), takes the collection into its
        //memory as it sets up and detaches its elements with their properties, which would need the first job's
        //transactions, when cap("m") reaches the master traversal, before it returns them as references
        final Map<String, List<Vertex>> byLabel = graph.traversal().withComputer().V().out("knows")
            .<String, List<Vertex>>group("m").by(T.label).pageRank(1).with(PageRank.times, 1)
            .<Map<String, List<Vertex>>>cap("m").next();

        assertEquals(Collections.singleton("vertex"), byLabel.keySet());
        assertEquals(knownIds, byLabel.get("vertex").stream().map(Vertex::id).collect(Collectors.toSet()));
        graph.tx().rollback();
    }

    @Test
    public void aGraphRegistersANewIndexAfterAnOlapJob() throws Exception {
        graph = openGraph(graphBuilder(), VERTICES);
        graph.compute().program(new OLAPTest.DegreeCounter()).submit().get().close();

        final JanusGraphManagement mgmt = graph.openManagement();
        mgmt.buildIndex("byName", Vertex.class).addKey(mgmt.getPropertyKey("name")).buildCompositeIndex();
        mgmt.commit();

        //The instance acknowledges the change once the transactions which were open when it arrived have closed
        final GraphIndexStatusReport report = ManagementSystem.awaitGraphIndexStatus(graph, "byName")
            .timeout(30, ChronoUnit.SECONDS).call();
        assertTrue(report.getSucceeded(), report.toString());
    }

    @Test
    public void olapJobsWhichRunAtOnceCreateTheirComputeKeyOnce() throws Exception {
        //Rounds of four jobs submitted at once, until the commits of two of them met on the lock of the key's name,
        //within a budget of 30 s; a round takes about 3 s
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        boolean met = false;
        while (!met && System.nanoTime() < deadline) {
            final List<String> retries = new CopyOnWriteArrayList<>();
            captureDebug(FulgoraGraphComputer.class, "Could not take the lock", retries, () -> runJobsAtOnce(4));
            met = !retries.isEmpty();
        }
        assertTrue(met, "the jobs never created the key at the same time");
    }

    private void runJobsAtOnce(int jobs) {
        if (graph != null) {
            graph.close();
        }
        //A commit holds the lock of the key's name for at least a second, which the other jobs' commits meet
        graph = openGraph(graphBuilder().set("storage.lock.wait-time", 1000), VERTICES);
        final List<Future<ComputerResult>> futures = new ArrayList<>();
        for (int i = 0; i < jobs; i++) {
            futures.add(graph.compute().program(new OLAPTest.DegreeCounter()).submit());
        }
        final List<Throwable> failures = new ArrayList<>();
        for (Future<ComputerResult> future : futures) {
            try {
                future.get().close();
            } catch (ExecutionException e) {
                failures.add(e.getCause());
            } catch (Exception e) {
                failures.add(e);
            }
        }
        assertEquals(Collections.emptyList(), failures);

        final JanusGraphManagement mgmt = graph.openManagement();
        try {
            assertEquals(1, StreamSupport.stream(mgmt.getRelationTypes(PropertyKey.class).spliterator(), false)
                .filter(key -> key.name().equals(OLAPTest.DegreeCounter.DEGREE)).count());
        } finally {
            mgmt.rollback();
        }
    }

    @Test
    public void aManagementSystemWhoseCommitFailedRollsBack() {
        graph = openGraph(graphBuilder(), 0);
        final JanusGraphManagement first = graph.openManagement();
        final JanusGraphManagement second = graph.openManagement();
        first.makePropertyKey("age").dataType(Integer.class).make();
        second.makePropertyKey("age").dataType(Integer.class).make();
        first.commit();

        final JanusGraphException failure = assertThrows(JanusGraphException.class, second::commit);
        assertTrue(failure.isCausedBy(PermanentLockingException.class), failure.toString());
        //Rather than an exception about the transaction, which the failed commit has closed already
        second.rollback();
        assertFalse(second.isOpen());
    }

    //Collects every vertex it runs on in its memory, or, in a second iteration, the vertices which those know: the scan
    //of an iteration preloads the edges of the scopes declared in the one before, so the first iteration sees none
    private static class CollectingProgram extends StaticVertexProgram<Object> {

        static final String VERTICES_KEY = "vertices";

        private final boolean adjacent;

        CollectingProgram(boolean adjacent) {
            this.adjacent = adjacent;
        }

        @Override
        public void setup(Memory memory) {
            memory.set(VERTICES_KEY, new ArrayList<>());
        }

        @Override
        public void execute(Vertex vertex, Messenger<Object> messenger, Memory memory) {
            if (!adjacent) {
                memory.add(VERTICES_KEY, Collections.singletonList(vertex));
            } else if (!memory.isInitialIteration()) {
                vertex.vertices(Direction.OUT, "knows")
                    .forEachRemaining(known -> memory.add(VERTICES_KEY, Collections.singletonList(known)));
            }
        }

        @Override
        public boolean terminate(Memory memory) {
            return !adjacent || memory.getIteration() >= 1;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return Collections.singleton(MemoryComputeKey.of(VERTICES_KEY, Operator.addAll, false, false));
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            //The next iteration's scan preloads the edges of the scope's reverse direction, the out-edges for this
            //one, which the program follows to the known vertices; it sends no messages
            return adjacent && memory.getIteration() < 1
                ? Collections.singleton(MessageScope.Local.of(__::inE)) : Collections.emptySet();
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

    //Runs the test with the debug messages which the class logs meanwhile and which start with the prefix collected; the
    //tests route SLF4J to Log4j 2
    private static void captureDebug(Class<?> logging, String prefix, List<String> messages, Runnable test) {
        final String name = logging.getName();
        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        final Configuration configuration = context.getConfiguration();
        final LoggerConfig existing = configuration.getLoggerConfig(name);
        final boolean ownConfig = name.equals(existing.getName());
        final LoggerConfig config = ownConfig ? existing : new LoggerConfig(name, Level.DEBUG, true);
        final Level level = config.getLevel();
        final AbstractAppender appender = new AbstractAppender("debug-" + UUID.randomUUID(), null, null, true,
                Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                if (event.getLevel() == Level.DEBUG && event.getMessage().getFormattedMessage().startsWith(prefix)) {
                    messages.add(event.getMessage().getFormattedMessage());
                }
            }
        };
        appender.start();
        if (ownConfig) {
            config.setLevel(Level.DEBUG);
        } else {
            configuration.addLogger(name, config);
        }
        config.addAppender(appender, Level.DEBUG, null);
        context.updateLoggers();
        try {
            test.run();
        } finally {
            config.removeAppender(appender.getName());
            appender.stop();
            if (ownConfig) {
                config.setLevel(level);
            } else {
                configuration.removeLogger(name);
            }
            context.updateLoggers();
        }
    }
}
