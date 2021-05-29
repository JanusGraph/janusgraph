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

package org.janusgraph.olap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponent;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphComputer;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.Transaction;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.olap.QueryContainer;
import org.janusgraph.graphdb.olap.VertexJobConverter;
import org.janusgraph.graphdb.olap.VertexScanJob;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.janusgraph.graphdb.olap.job.GhostVertexRemover;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class OLAPTest extends JanusGraphBaseTest {

    private static final Random random = new Random();

    private static final Logger log =
            LoggerFactory.getLogger(OLAPTest.class);

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
    }

    private ScanMetrics executeScanJob(VertexScanJob job) throws Exception {
        return executeScanJob(VertexJobConverter.convert(graph,job));
    }

    private ScanMetrics executeScanJob(ScanJob job) throws Exception {
        return graph.getBackend().buildEdgeScanJob()
                .setNumProcessingThreads(2)
                .setWorkBlockSize(100)
                .setJob(job)
                .execute().get();
    }

    private int generateRandomGraph(int numV) {
        mgmt.makePropertyKey("uid").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makePropertyKey("values").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.makePropertyKey("numvals").dataType(Integer.class).make();
        finishSchema();
        int numE = 0;
        JanusGraphVertex[] vs = new JanusGraphVertex[numV];
        for (int i=0;i<numV;i++) {
            vs[i] = tx.addVertex("uid",i+1);
            int numberOfValues = random.nextInt(5)+1;
            vs[i].property(VertexProperty.Cardinality.single, "numvals", numberOfValues);
            for (int j=0;j<numberOfValues;j++) {
                vs[i].property("values",random.nextInt(100));
            }
        }
        for (int i=0;i<numV;i++) {
            int edges = i+1;
            JanusGraphVertex v = vs[i];
            for (int j=0;j<edges;j++) {
                JanusGraphVertex u = vs[random.nextInt(numV)];
                v.addEdge("knows", u);
                numE++;
            }
        }
        assertEquals(numV*(numV+1),numE*2);
        return numE;
    }

    @Test
    public void scannerShouldSeeAllVertices() throws Exception {
        GraphTraversalSource g = graph.traversal();
        Vertex v1 = g.addV().next();
        Vertex v2 = g.addV().next();
        g.V(v1).addE("connect").to(v2).iterate();
        g.addV().addV().addV().property("p", "v").iterate();
        g.tx().commit();

        AtomicInteger vertexNum = new AtomicInteger();
        executeScanJob(new VertexScanJob() {
            @Override
            public void process(final JanusGraphVertex vertex, final ScanMetrics metrics) {
                vertexNum.incrementAndGet();
            }

            @Override
            public void getQueries(final QueryContainer queries) {
                queries.addQuery().properties();
                queries.addQuery().edges();
            }

            @Override
            public VertexScanJob clone() {
                return this;
            }
        });

        assertEquals(g.V().count().next(), vertexNum.get());
    }

    @Test
    public void testVertexScan() throws Exception {
        int numV = 100;
        int numE = generateRandomGraph(numV);
        final String DEGREE_COUNT = "degree";
        final String VERTEX_COUNT = "numV";
        clopen();

        ScanMetrics result1 = executeScanJob(new VertexScanJob() {

            @Override
            public void process(JanusGraphVertex vertex, ScanMetrics metrics) {
                long outDegree = vertex.query().labels("knows").direction(Direction.OUT).edgeCount();
                assertEquals(0, vertex.query().labels("knows").direction(Direction.IN).edgeCount());
                assertEquals(1, vertex.query().labels("uid").propertyCount());
                assertTrue(vertex.<Integer>property("uid").orElse(0) > 0);
                metrics.incrementCustom(DEGREE_COUNT,outDegree);
                metrics.incrementCustom(VERTEX_COUNT);
            }

            @Override
            public void getQueries(QueryContainer queries) {
                queries.addQuery().labels("knows").direction(Direction.OUT).edges();
                queries.addQuery().keys("uid").properties();
            }

            @Override
            public VertexScanJob clone() { return this; }
        });
        assertEquals(numV,result1.getCustom(VERTEX_COUNT));
        assertEquals(numE,result1.getCustom(DEGREE_COUNT));

        ScanMetrics result2 = executeScanJob(new VertexScanJob() {

            @Override
            public void process(JanusGraphVertex vertex, ScanMetrics metrics) {
                metrics.incrementCustom(VERTEX_COUNT);
                assertEquals(1 ,vertex.query().labels("numvals").propertyCount());
                int numvals = vertex.value("numvals");
                assertEquals(numvals, vertex.query().labels("values").propertyCount());
            }

            @Override
            public void getQueries(QueryContainer queries) {
                queries.addQuery().keys("values").properties();
                queries.addQuery().keys("numvals").properties();
            }

            @Override
            public VertexScanJob clone() { return this; }
        });
        assertEquals(numV,result2.getCustom(VERTEX_COUNT));
    }

    @Test
    public void removeGhostVertices() throws Exception {
        JanusGraphVertex v1 = tx.addVertex("person");
        v1.property("name","stephen");
        JanusGraphVertex v2 = tx.addVertex("person");
        v1.property("name","marko");
        JanusGraphVertex v3 = tx.addVertex("person");
        v1.property("name","dan");
        v2.addEdge("knows",v3);
        v1.addEdge("knows",v2);
        newTx();
        long v3id = getId(v3);
        long v1id = getId(v1);
        assertTrue(v3id>0);

        v3 = getV(tx, v3id);
        assertNotNull(v3);
        v3.remove();
        tx.commit();

        JanusGraphTransaction xx = graph.buildTransaction().checkExternalVertexExistence(false).start();
        v3 = getV(xx, v3id);
        assertNotNull(v3);
        v1 = getV(xx, v1id);
        assertNotNull(v1);
        v3.property("name", "deleted");
        v3.addEdge("knows", v1);
        xx.commit();

        newTx();
        assertNull(getV(tx,v3id));
        v1 = getV(tx, v1id);
        assertNotNull(v1);
        assertEquals(v3id, v1.query().direction(Direction.IN).labels("knows").vertices().iterator().next().longId());
        tx.commit();
        mgmt.commit();

        ScanMetrics result = executeScanJob(new GhostVertexRemover(graph));
        assertEquals(1, result.getCustom(GhostVertexRemover.REMOVED_VERTEX_COUNT));
        assertEquals(2, result.getCustom(GhostVertexRemover.REMOVED_RELATION_COUNT));
        assertEquals(0, result.getCustom(GhostVertexRemover.SKIPPED_GHOST_LIMIT_COUNT));

        // Second scan should not find any ghost vertices
        result = executeScanJob(new GhostVertexRemover(graph));
        assertEquals(0, result.getCustom(GhostVertexRemover.REMOVED_VERTEX_COUNT));
        assertEquals(0, result.getCustom(GhostVertexRemover.REMOVED_RELATION_COUNT));
        assertEquals(0, result.getCustom(GhostVertexRemover.SKIPPED_GHOST_LIMIT_COUNT));
    }

    @Test
    public void testBasicComputeJob() {
        GraphTraversalSource g = graph.traversal().withComputer(FulgoraGraphComputer.class);
        System.out.println(g.V().count().next());
    }

    @Test
    public void degreeCounting() throws Exception {
        int numV = 200;
        int numE = generateRandomGraph(numV);
        clopen();

        final JanusGraphComputer computer = graph.compute();
        computer.resultMode(JanusGraphComputer.ResultMode.NONE);
        computer.workers(4);
        computer.program(new DegreeCounter());
        computer.mapReduce(new DegreeMapper());
        ComputerResult result = computer.submit().get();
        System.out.println("Execution time (ms) ["+numV+"|"+numE+"]: " + result.memory().getRuntime());
        assertTrue(result.memory().exists(DegreeMapper.DEGREE_RESULT));
        Map<Long,Integer> degrees = result.memory().get(DegreeMapper.DEGREE_RESULT);
        assertNotNull(degrees);
        assertEquals(numV,degrees.size());
        int totalCount = 0;
        for (Map.Entry<Long,Integer> entry : degrees.entrySet()) {
            int degree = entry.getValue();
            final JanusGraphVertex v = getV(tx, entry.getKey());
            int count = v.value("uid");
            assertEquals(count,degree);
            totalCount+= degree;
        }
        assertEquals(numV*(numV+1)/2,totalCount);
        assertEquals(1,result.memory().getIteration());
    }

    @Test
    public void vertexProgramExceptionPropagatesToCaller() throws InterruptedException
    {
        int numV = 100;
        generateRandomGraph(numV);
        clopen();

        final JanusGraphComputer computer = graph.compute();
        computer.resultMode(JanusGraphComputer.ResultMode.NONE);
        computer.workers(1);
        computer.program(new ExceptionProgram());

        try {
            computer.submit().get();
            fail();
        } catch (ExecutionException ignored) {
        }
    }

    @Test
    public void degreeCountingDistance() throws Exception {
        int numV = 100;
        int numE = generateRandomGraph(numV);
        clopen();

        // TODO does this iteration over JanusGraphComputer.ResultMode values imply that DegreeVariation's ResultGraph/Persist should also change?
        for (JanusGraphComputer.ResultMode mode : JanusGraphComputer.ResultMode.values()) {
            final JanusGraphComputer computer = graph.compute();
            computer.resultMode(mode);
            computer.workers(1);
            computer.program(new DegreeCounter(2));
            ComputerResult result = computer.submit().get();
            System.out.println("Execution time (ms) ["+numV+"|"+numE+"]: " + result.memory().getRuntime());
            assertEquals(2,result.memory().getIteration());

            Transaction gview = null;
            switch (mode) {
                case LOCALTX: gview = (Transaction) result.graph(); break;
                case PERSIST: newTx(); gview = tx; break;
                case NONE: break;
                default: throw new AssertionError(mode);
            }
            if (gview == null) continue;

            for (JanusGraphVertex v : gview.query().vertices()) {
                long degree2 = ((Integer)v.value(DegreeCounter.DEGREE)).longValue();
                long actualDegree2 = 0;
                for (JanusGraphVertex w : v.query().direction(Direction.OUT).vertices()) {
                    actualDegree2 += Iterables.size(w.query().direction(Direction.OUT).vertices());
                }
                assertEquals(actualDegree2,degree2);
            }
            if (mode== JanusGraphComputer.ResultMode.LOCALTX) {
                assertTrue(gview instanceof JanusGraphTransaction);
                ((JanusGraphTransaction) gview).rollback();
            }
        }
    }

    public static class ExceptionProgram extends StaticVertexProgram<Integer>
    {

        @Override
        public void setup(Memory memory)
        {

        }

        @Override
        public void execute(Vertex vertex, Messenger<Integer> messenger, Memory memory)
        {
            throw new NullPointerException();
        }

        @Override
        public boolean terminate(Memory memory)
        {
            return memory.getIteration() > 1;
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory)
        {
            return ImmutableSet.of();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }

        @Override
        public Features getFeatures() {
            return new Features() {
                @Override
                public boolean requiresLocalMessageScopes() {
                    return true;
                }

                @Override
                public boolean requiresVertexPropertyAddition() {
                    return true;
                }
            };
        }
    }

    public static class DegreeCounter extends StaticVertexProgram<Integer> {

        public static final String DEGREE = "degree";
        public static final MessageScope.Local<Integer> DEG_MSG = MessageScope.Local.of(__::inE);

        private final int length;

        public DegreeCounter() {
            this(1);
        }

        public DegreeCounter(int length) {
            Preconditions.checkArgument(length>0);
            this.length = length;
        }

        @Override
        public void setup(Memory memory) {
        }

        @Override
        public void execute(Vertex vertex, Messenger<Integer> messenger, Memory memory) {
            if (memory.isInitialIteration()) {
                messenger.sendMessage(DEG_MSG, 1);
            } else {
                int degree = IteratorUtils.stream(messenger.receiveMessages()).reduce(0, Integer::sum);
                vertex.property(VertexProperty.Cardinality.single, DEGREE, degree);
                if (memory.getIteration()<length) messenger.sendMessage(DEG_MSG, degree);
            }
        }

        @Override
        public boolean terminate(Memory memory) {
            return memory.getIteration()>=length;
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return new HashSet<>(Collections.singletonList(VertexComputeKey.of(DEGREE, false)));
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return new HashSet<>(Collections.singletonList(MemoryComputeKey.of(DEGREE, Operator.assign, true, false)));
        }

        @Override
        public Optional<MessageCombiner<Integer>> getMessageCombiner() {
            return Optional.of(Integer::sum);
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            if (memory.getIteration()<length) return ImmutableSet.of(DEG_MSG);
            else return Collections.emptySet();
        }

        // TODO i'm not sure these preferences are correct

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }

        @Override
        public Features getFeatures() {
            return new Features() {
                @Override
                public boolean requiresLocalMessageScopes() {
                    return true;
                }

                @Override
                public boolean requiresVertexPropertyAddition() {
                    return true;
                }
            };
        }


    }

    public static class DegreeMapper extends StaticMapReduce<Long,Integer,Long,Integer,Map<Long,Integer>> {

        public static final String DEGREE_RESULT = "degrees";

        @Override
        public boolean doStage(Stage stage) {
            return stage==Stage.MAP;
        }

        @Override
        public void map(Vertex vertex, MapEmitter<Long, Integer> emitter) {
            emitter.emit((Long)vertex.id(),vertex.value(DegreeCounter.DEGREE));
        }

        @Override
        public Map<Long, Integer> generateFinalResult(Iterator<KeyValue<Long, Integer>> keyValues) {
            Map<Long,Integer> result = new HashMap<>();
            while (keyValues.hasNext()) {
                KeyValue<Long, Integer> r =  keyValues.next();
                result.put(r.getKey(),r.getValue());
            }
            return result;
        }

        @Override
        public String getMemoryKey() {
            return DEGREE_RESULT;
        }

    }

    public static class Degree {
        public int in;
        public int out;
        public int both;
        public int prop;

        public Degree(int in, int out,int prop) {
            this.in=in;
            this.out=out;
            both=in+out;
            this.prop = prop;
        }

        public Degree() {
            this(0,0,0);
        }

        public void add(Degree d) {
            in+=d.in;
            out+=d.out;
            both+=d.both;
            prop+=d.prop;
        }

    }


    private void expand(Vertex v, final int distance, final int diameter, final int branch) {
        v.property(VertexProperty.Cardinality.single, "distance", distance);
        if (distance<diameter) {
//          JanusGraphVertex previous = null;
            for (int i=0;i<branch;i++) {
                JanusGraphVertex u = tx.addVertex();
                u.addEdge("likes",v);
                log.debug("likes {}->{}", u.id(), v.id());
                // Commented since the PageRank implementation does not discriminate by label
//                if (previous!=null) {
//                    u.addEdge("knows",previous);
//                    log.error("knows {}->{}", u.id(), v.id());
//                }
//              previous=u;
                expand(u,distance+1,diameter,branch);
            }
        }
    }

    @Test
    public void testPageRank() throws ExecutionException, InterruptedException {
        mgmt.makePropertyKey("distance").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("likes").multiplicity(Multiplicity.MULTI).make();
        finishSchema();
        final int branch = 6;
        final int diameter = 5;
        final double alpha = 0.85d;
        int numV = (int)((Math.pow(branch,diameter+1)-1)/(branch-1));
        JanusGraphVertex v = tx.addVertex();
        expand(v,0,diameter,branch);
        clopen();
        assertCount(numV, tx.query().vertices());
        log.debug("PR test numV: {}", numV);
        newTx();

        //Precompute correct PR results:
        double[] correctPR = new double[diameter+1];
        for (int i=diameter;i>=0;i--) {
            double pr = (1.0D - alpha)/numV;
            if (i<diameter) pr+= alpha*branch*correctPR[i+1];
            log.debug("diameter={} pr={}", diameter, pr);
            correctPR[i]=pr;
        }

        double correctPRSum = 0;
        for (final JanusGraphVertex janusGraphVertex : tx.query().vertices()) {
            correctPRSum += correctPR[janusGraphVertex.<Integer>value("distance")];
        }

        final JanusGraphComputer computer = graph.compute();
        computer.resultMode(JanusGraphComputer.ResultMode.NONE);
        computer.workers(4);
        computer.program(PageRankVertexProgram.build().iterations(10).vertexCount(numV).dampingFactor(alpha).create(graph));
        computer.mapReduce(PageRankMapReduce.build().create());
        ComputerResult result = computer.submit().get();

        Iterator<KeyValue<Long, Double>> ranks = result.memory().get(PageRankMapReduce.DEFAULT_MEMORY_KEY);
        assertNotNull(ranks);
        int vertexCounter = 0;
        double computedPRSum = 0;
        correctPRSum = 0;
        final Set<Long> vertexIDs = new HashSet<>(numV);
        while (ranks.hasNext()) {
            final KeyValue<Long, Double> rank = ranks.next();
            final Long vertexID = rank.getKey();
            final Double computedPR = rank.getValue();
            assertNotNull(vertexID);
            assertNotNull(computedPR);
            final JanusGraphVertex u = getV(tx, vertexID);
            final int distance = u.<Integer>value("distance");
            vertexCounter++;

            //assertEquals("Incorrect PR on vertex #" + vertexCounter, correctPR[distance], computedPR, EPSILON);
            computedPRSum += computedPR;
            correctPRSum += correctPR[distance];

            assertFalse(vertexIDs.contains(vertexID));
            vertexIDs.add(vertexID);

            log.debug("vertexID={} computedPR={}", vertexID, computedPR);
        }

        assertEquals(numV, vertexCounter);
        assertEquals(correctPRSum, computedPRSum, 0.001);
    }

    @Test
    public void testShortestDistance() throws Exception {
        PropertyKey distance = mgmt.makePropertyKey("distance").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("connect").signature(distance).multiplicity(Multiplicity.MULTI).make();
        finishSchema();

        int maxDepth = 16;
        int maxBranch = 5;
        JanusGraphVertex vertex = tx.addVertex();
        //Grow a star-shaped graph around vertex which will be the single-source for this shortest path computation
        final int numV = growVertex(vertex,0,maxDepth, maxBranch);
        final int numE = numV-1;
        assertCount(numV,tx.query().vertices());
        assertCount(numE,tx.query().edges());

        log.debug("seed inE count: {}", vertex.query().direction(Direction.IN).edgeCount());
        log.debug("seed outE count: {}", vertex.query().direction(Direction.OUT).edgeCount());

        clopen();

        final JanusGraphComputer computer = graph.compute();
        computer.resultMode(JanusGraphComputer.ResultMode.NONE);
        computer.workers(4);
        computer.program(ShortestDistanceVertexProgram.build().seed((long)vertex.id()).maxDepth(maxDepth + 4).create(graph));
        computer.mapReduce(ShortestDistanceMapReduce.build().create());
        ComputerResult result = computer.submit().get();

        Iterator<KeyValue<Long, Long>> distances =
                result.memory().get(ShortestDistanceMapReduce.DEFAULT_MEMORY_KEY);

        int vertexCount = 0;

        while (distances.hasNext()) {
            final KeyValue<Long, Long> kv = distances.next();
            final long dist = kv.getValue();
            assertTrue(dist >= 0 && dist < Integer.MAX_VALUE, "Invalid distance: " + dist);
            JanusGraphVertex v = getV(tx, kv.getKey());
            assertEquals(v.<Integer>value("distance").intValue(), dist);
            vertexCount++;
        }

        assertEquals(numV, vertexCount);
        assertTrue(0 < vertexCount);
    }

    private int growVertex(Vertex vertex, int depth, int maxDepth, int maxBranch) {
        vertex.property(VertexProperty.Cardinality.single, "distance", depth);
        int total=1;
        if (depth>=maxDepth) return total;

        for (int i=0;i<random.nextInt(maxBranch)+1;i++) {
            int dist = random.nextInt(3)+1;
            JanusGraphVertex n = tx.addVertex();
            n.addEdge("connect",vertex, "distance",dist);
            total+=growVertex(n,depth+dist,maxDepth,maxBranch);
        }
        return total;
    }

    @Test
    public void testShortestPath() {
        GraphTraversalSource g = graph.traversal();
        Vertex v1 = g.addV().next();
        Vertex v2 = g.addV().next();
        Vertex v3 = g.addV().next();
        Vertex v4 = g.addV().next();
        g.V(v1).addE("E").to(v2).iterate();
        g.V(v1).addE("E").to(v3).iterate();
        g.V(v2).addE("E").to(v4).iterate();
        g.V(v3).addE("E").to(v4).iterate();
        g.tx().commit();

        g = graph.traversal().withComputer();
        List<Path> paths = g.V(v1).shortestPath().with(ShortestPath.target, __.is(v2)).toList();

        assertCount(1, paths);
        assertEquals(2, paths.get(0).size());
    }

    @Test
    public void testConnectedComponent() {
        createComponentWithThreeVertices();
        newTx();
        GraphTraversalSource g = graph.traversal();
        Vertex isolatedVertex = g.addV().property("id", -1).next();
        g.tx().commit();
        g = graph.traversal().withComputer(FulgoraGraphComputer.class);

        GraphTraversal<Vertex, Map<String, Object>> traversal =
            g.V().connectedComponent().project("id", "component").by("id").by(ConnectedComponent.component);

        boolean foundIsolatedVertex = false;
        List<String> nonIsolatedComponents = new ArrayList<>();
        while (traversal.hasNext()) {
            Map<String, Object> m = traversal.next();
            if (m.get("component").equals(isolatedVertex.id().toString())) {
                foundIsolatedVertex = true;
            } else {
                nonIsolatedComponents.add((String) m.get("component"));
            }
        }
        assertTrue(foundIsolatedVertex);
        assertEquals(3, nonIsolatedComponents.size());
        assertEquals(nonIsolatedComponents.get(0), nonIsolatedComponents.get(1));
        assertEquals(nonIsolatedComponents.get(1), nonIsolatedComponents.get(2));
    }

    private void createComponentWithThreeVertices() {
        mgmt.makePropertyKey("id").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").make();
        finishSchema();

        GraphTraversalSource g = graph.traversal();
        Vertex v1 = g.addV().property("id", 0).next();
        Vertex v2 = g.addV().property("id", 1).next();
        Vertex v3 = g.addV().property("id", 2).next();

        g.V(v1).addE("knows").to(v2).iterate();
        g.V(v2).addE("knows").to(v3).iterate();

        tx.commit();
    }
}
