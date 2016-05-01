package com.thinkaurelius.titan.olap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.olap.*;
import com.thinkaurelius.titan.graphdb.olap.computer.FulgoraGraphComputer;
import com.thinkaurelius.titan.graphdb.olap.job.GhostVertexRemover;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.thinkaurelius.titan.testutil.TitanAssert.assertCount;
import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class OLAPTest extends TitanGraphBaseTest {

    private static final double EPSILON = 0.00001;

    private static final Random random = new Random();

    private static final Logger log =
            LoggerFactory.getLogger(OLAPTest.class);

    @Before
    public void setUp() throws Exception {
        super.setUp();
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
        TitanVertex[] vs = new TitanVertex[numV];
        for (int i=0;i<numV;i++) {
            vs[i] = tx.addVertex("uid",i+1);
            int numVals = random.nextInt(5)+1;
            vs[i].property(VertexProperty.Cardinality.single, "numvals", numVals);
            for (int j=0;j<numVals;j++) {
                vs[i].property("values",random.nextInt(100));
            }
        }
        for (int i=0;i<numV;i++) {
            int edges = i+1;
            TitanVertex v = vs[i];
            for (int j=0;j<edges;j++) {
                TitanVertex u = vs[random.nextInt(numV)];
                v.addEdge("knows", u);
                numE++;
            }
        }
        assertEquals(numV*(numV+1),numE*2);
        return numE;
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
            public void process(TitanVertex vertex, ScanMetrics metrics) {
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
            public void process(TitanVertex vertex, ScanMetrics metrics) {
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
        TitanVertex v1 = tx.addVertex("person");
        v1.property("name","stephen");
        TitanVertex v2 = tx.addVertex("person");
        v1.property("name","marko");
        TitanVertex v3 = tx.addVertex("person");
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

        TitanTransaction xx = graph.buildTransaction().checkExternalVertexExistence(false).start();
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
        assertEquals(v3id,v1.query().direction(Direction.IN).labels("knows").vertices().iterator().next().longId());
        tx.commit();
        mgmt.commit();

        ScanMetrics result = executeScanJob(new GhostVertexRemover(graph));
        assertEquals(1,result.getCustom(GhostVertexRemover.REMOVED_VERTEX_COUNT));
        assertEquals(2,result.getCustom(GhostVertexRemover.REMOVED_RELATION_COUNT));
        assertEquals(0,result.getCustom(GhostVertexRemover.SKIPPED_GHOST_LIMIT_COUNT));
    }

    @Test
    public void testBasicComputeJob() throws Exception {
        GraphTraversalSource g = graph.traversal().withComputer(FulgoraGraphComputer.class);
        System.out.println(g.V().count().next());
    }

    @Test
    public void degreeCounting() throws Exception {
        int numV = 200;
        int numE = generateRandomGraph(numV);
        clopen();

        final FulgoraGraphComputer computer = graph.compute();
        computer.resultMode(FulgoraGraphComputer.ResultMode.NONE);
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
            TitanVertex v = getV(tx, entry.getKey().longValue());
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
        int numE = generateRandomGraph(numV);
        clopen();

        final FulgoraGraphComputer computer = graph.compute();
        computer.resultMode(FulgoraGraphComputer.ResultMode.NONE);
        computer.workers(1);
        computer.program(new ExceptionProgram());

        try {
            computer.submit().get();
            fail();
        } catch (ExecutionException ee) {
        }
    }

    @Test
    public void degreeCountingDistance() throws Exception {
        int numV = 100;
        int numE = generateRandomGraph(numV);
        clopen();

        // TODO does this iteration over FulgoraGraphComputer.ResultMode values imply that DegreeVariation's ResultGraph/Persist should also change?
        for (FulgoraGraphComputer.ResultMode mode : FulgoraGraphComputer.ResultMode.values()) {
            final FulgoraGraphComputer computer = graph.compute();
            computer.resultMode(mode);
            computer.workers(1);
            computer.program(new DegreeCounter(2));
            ComputerResult result = computer.submit().get();
            System.out.println("Execution time (ms) ["+numV+"|"+numE+"]: " + result.memory().getRuntime());
            assertEquals(2,result.memory().getIteration());

            TitanGraphTransaction gview = null;
            switch (mode) {
                case LOCALTX: gview = (TitanGraphTransaction) result.graph(); break;
                case PERSIST: newTx(); gview = tx; break;
                case NONE: break;
                default: throw new AssertionError(mode);
            }
            if (gview == null) continue;

            for (TitanVertex v : gview.query().vertices()) {
                long degree2 = ((Integer)v.value(DegreeCounter.DEGREE)).longValue();
                long actualDegree2 = 0;
                for (TitanVertex w : v.query().direction(Direction.OUT).vertices()) {
                    actualDegree2 += Iterables.size(w.query().direction(Direction.OUT).vertices());
                }
                assertEquals(actualDegree2,degree2);
            }
            if (mode== FulgoraGraphComputer.ResultMode.LOCALTX) {
                assertTrue(gview instanceof TitanTransaction);
                ((TitanTransaction)gview).rollback();
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
        public static final MessageCombiner<Integer> ADDITION = (a,b) -> a+b;
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
            return;
        }

        @Override
        public void execute(Vertex vertex, Messenger<Integer> messenger, Memory memory) {
            if (memory.isInitialIteration()) {
                messenger.sendMessage(DEG_MSG, 1);
            } else {
                int degree = IteratorUtils.stream(messenger.receiveMessages()).reduce(0, (a, b) -> a + b);
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
            return new HashSet<>(Arrays.asList(VertexComputeKey.of(DEGREE, false)));
        }

	@Override
	public Set<MemoryComputeKey> getMemoryComputeKeys() {
	    return new HashSet<>(Arrays.asList(MemoryComputeKey.of(DEGREE, Operator.assign, true, false)));
	}

        @Override
        public Optional<MessageCombiner<Integer>> getMessageCombiner() {
            return Optional.of(ADDITION);
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            if (memory.getIteration()<length) return ImmutableSet.of((MessageScope)DEG_MSG);
            else return Collections.EMPTY_SET;
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
            for (; keyValues.hasNext(); ) {
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
            TitanVertex previous = null;
            for (int i=0;i<branch;i++) {
                TitanVertex u = tx.addVertex();
                u.addEdge("likes",v);
                log.debug("likes {}->{}", u.id(), v.id());
                // Commented since the PageRank implementation does not discriminate by label
//                if (previous!=null) {
//                    u.addEdge("knows",previous);
//                    log.error("knows {}->{}", u.id(), v.id());
//                }
                previous=u;
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
        TitanVertex v = tx.addVertex();
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
        Iterator<TitanVertex> iv = tx.query().vertices().iterator();
        while (iv.hasNext()) {
            correctPRSum += correctPR[iv.next().<Integer>value("distance")];
        }

        final FulgoraGraphComputer computer = graph.compute();
        computer.resultMode(FulgoraGraphComputer.ResultMode.NONE);
        computer.workers(4);
        computer.program(PageRankVertexProgram.build().iterations(10).vertexCount(numV).dampingFactor(alpha).create(graph));
        computer.mapReduce(PageRankMapReduce.build().create());
        ComputerResult result = computer.submit().get();

        Iterator<KeyValue<Long, Double>> ranks = result.memory().get(PageRankMapReduce.DEFAULT_MEMORY_KEY);
        assertNotNull(ranks);
        int vertexCounter = 0;
        double computedPRSum = 0;
        correctPRSum = 0;
        Set<Long> vertexIDs = new HashSet<Long>(numV);
        while (ranks.hasNext()) {
            final KeyValue<Long, Double> rank = ranks.next();
            final Long vertexID = rank.getKey();
            final Double computedPR = rank.getValue();
            assertNotNull(vertexID);
            assertNotNull(computedPR);
            final TitanVertex u = getV(tx, vertexID);
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
        TitanVertex vertex = tx.addVertex();
        //Grow a star-shaped graph around vertex which will be the single-source for this shortest path computation
        final int numV = growVertex(vertex,0,maxDepth, maxBranch);
        final int numE = numV-1;
        assertCount(numV,tx.query().vertices());
        assertCount(numE,tx.query().edges());

        log.debug("seed inE count: {}", vertex.query().direction(Direction.IN).edgeCount());
        log.debug("seed outE count: {}", vertex.query().direction(Direction.OUT).edgeCount());

        clopen();

        final FulgoraGraphComputer computer = graph.compute();
        computer.resultMode(FulgoraGraphComputer.ResultMode.NONE);
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
            assertTrue("Invalid distance: " + dist,dist >= 0 && dist < Integer.MAX_VALUE);
            TitanVertex v = getV(tx, kv.getKey());
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
            TitanVertex n = tx.addVertex();
            n.addEdge("connect",vertex, "distance",dist);
            total+=growVertex(n,depth+dist,maxDepth,maxBranch);
        }
        return total;
    }


}
