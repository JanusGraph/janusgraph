package com.thinkaurelius.titan.olap;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.olap.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.olap.QueryContainer;
import com.thinkaurelius.titan.graphdb.olap.VertexJobConverter;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.graphdb.olap.job.GhostVertexRemover;
import com.tinkerpop.gremlin.process.computer.*;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.junit.Assert.*;
import static com.thinkaurelius.titan.testutil.TitanAssert.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class OLAPTest extends TitanGraphBaseTest {

    private static final double EPSILON = 0.00001;

    protected abstract <S> OLAPJobBuilder<S> getOLAPBuilder(StandardTitanGraph graph, Class<S> clazz);

    private static final Random random = new Random();

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
        Vertex[] vs = new Vertex[numV];
        for (int i=0;i<numV;i++) {
            vs[i] = tx.addVertex("uid",i+1);
            int numVals = random.nextInt(5)+1;
            vs[i].singleProperty("numvals",numVals);
            for (int j=0;j<numVals;j++) {
                vs[i].property("values",random.nextInt(100));
            }
        }
        for (int i=0;i<numV;i++) {
            int edges = i+1;
            Vertex v = vs[i];
            for (int j=0;j<edges;j++) {
                Vertex u = vs[random.nextInt(numV)];
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
                long outDegree = vertex.outE("knows").count().next();
                assertEquals(0,vertex.inE("knows").count().next().longValue());
                assertEquals(1,vertex.properties("uid").count().next().longValue());
                assertTrue(vertex.<Integer>value("uid",0) > 0);
                metrics.incrementCustom(DEGREE_COUNT,outDegree);
                metrics.incrementCustom(VERTEX_COUNT);
            }

            @Override
            public void getQueries(QueryContainer queries) {
                queries.addQuery().labels("knows").direction(Direction.OUT).edges();
                queries.addQuery().keys("uid").properties();
            }
        });
        assertEquals(numV,result1.getCustom(VERTEX_COUNT));
        assertEquals(numE,result1.getCustom(DEGREE_COUNT));

        ScanMetrics result2 = executeScanJob(new VertexScanJob() {

            @Override
            public void process(TitanVertex vertex, ScanMetrics metrics) {
                metrics.incrementCustom(VERTEX_COUNT);
                assertEquals(1,vertex.properties("numvals").count().next().longValue());
                int numvals = vertex.value("numvals");
                assertEquals(numvals,vertex.properties("values").count().next().longValue());
            }

            @Override
            public void getQueries(QueryContainer queries) {
                queries.addQuery().keys("values").properties();
                queries.addQuery().keys("numvals").properties();
            }
        });
        assertEquals(numV,result2.getCustom(VERTEX_COUNT));
    }

    @Test
    public void removeGhostVertices() throws Exception {
        Vertex v1 = tx.addVertex("person");
        v1.property("name","stephen");
        Vertex v2 = tx.addVertex("person");
        v1.property("name","marko");
        Vertex v3 = tx.addVertex("person");
        v1.property("name","dan");
        v2.addEdge("knows",v3);
        v1.addEdge("knows",v2);
        newTx();
        long v3id = getId(v3);
        long v1id = getId(v1);
        assertTrue(v3id>0);

        v3 = tx.v(v3id);
        assertNotNull(v3);
        v3.remove();
        tx.commit();

        TitanTransaction xx = graph.buildTransaction().checkExternalVertexExistence(false).start();
        v3 = xx.v(v3id);
        assertNotNull(v3);
        v1 = xx.v(v1id);
        assertNotNull(v1);
        v3.property("name","deleted");
        v3.addEdge("knows",v1);
        xx.commit();

        newTx();
        try {
            v3 = tx.v(v3id);
            fail();
        } catch (NoSuchElementException e) {}
        v1 = tx.v(v1id);
        assertNotNull(v1);
        assertEquals(v3id,v1.in("knows").next().id());
        tx.commit();
        mgmt.commit();

        ScanMetrics result = executeScanJob(new GhostVertexRemover(graph));
        assertEquals(1,result.getCustom(GhostVertexRemover.REMOVED_VERTEX_COUNT));
        assertEquals(2,result.getCustom(GhostVertexRemover.REMOVED_RELATION_COUNT));
        assertEquals(0,result.getCustom(GhostVertexRemover.SKIPPED_GHOST_LIMIT_COUNT));
    }

    @Test
    public void degreeCounting() throws Exception {
        int numV = 200;
        int numE = generateRandomGraph(numV);
        clopen();

        final TitanGraphComputer computer = graph.compute();
        computer.setResultMode(TitanGraphComputer.ResultMode.NONE);
        computer.setNumProcessingThreads(4);
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
            Vertex v = tx.v(entry.getKey().longValue());
            int count = v.value("uid");
            assertEquals(count,degree);
            totalCount+= degree;
        }
        assertEquals(numV*(numV+1)/2,totalCount);
        assertEquals(1,result.memory().getIteration());
    }

    @Test
    public void degreeCountingDistance() throws Exception {
        int numV = 100;
        int numE = generateRandomGraph(numV);
        clopen();

        for (TitanGraphComputer.ResultMode mode : TitanGraphComputer.ResultMode.values()) {
            final TitanGraphComputer computer = graph.compute();
            computer.setResultMode(mode);
            computer.setNumProcessingThreads(1);
            computer.program(new DegreeCounter(2));
            ComputerResult result = computer.submit().get();
            System.out.println("Execution time (ms) ["+numV+"|"+numE+"]: " + result.memory().getRuntime());
            assertEquals(2,result.memory().getIteration());

            Graph gview = null;
            switch (mode) {
                case LOCALTX: gview = result.graph(); break;
                case PERSIST: newTx(); gview = tx; break;
                case NONE: break;
                default: throw new AssertionError(mode);
            }
            if (gview == null) continue;

            for (Vertex v : gview.V().toList()) {
                long degree2 = ((Integer)v.value(DegreeCounter.DEGREE)).longValue();
                long actualDegree2 = v.out().out().count().next();
                assertEquals(actualDegree2,degree2);
            }
            if (mode== TitanGraphComputer.ResultMode.LOCALTX) {
                assertTrue(gview instanceof TitanTransaction);
                ((TitanTransaction)gview).rollback();
            }
        }
    }

    public static class DegreeCounter implements VertexProgram<Integer> {

        public static final String DEGREE = "degree";
        public static final MessageCombiner<Integer> ADDITION = (a,b) -> a+b;
        public static final MessageScope.Local<Integer> DEG_MSG = MessageScope.Local.of(() -> GraphTraversal.<Vertex>of().inE());

        private final int length;

        DegreeCounter() {
            this(1);
        }

        DegreeCounter(int length) {
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
                int degree = StreamFactory.stream(messenger.receiveMessages(DEG_MSG)).reduce(0, (a, b) -> a + b);
                vertex.singleProperty(DEGREE, degree);
                if (memory.getIteration()<length) messenger.sendMessage(DEG_MSG, degree);
            }
        }

        @Override
        public boolean terminate(Memory memory) {
            return memory.getIteration()>=length;
        }

        @Override
        public Set<String> getElementComputeKeys() {
            return ImmutableSet.of(DEGREE);
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

    public static class DegreeMapper implements MapReduce<Long,Integer,Long,Integer,Map<Long,Integer>> {

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


    @Test
    public void degreeCount() throws Exception {
        int numV = 300;
        int numE = generateRandomGraph(numV);
        clopen();



        Stopwatch w = new Stopwatch().start();
        final OLAPJobBuilder<Degree> builder = getOLAPBuilder(graph,Degree.class);
        OLAPResult<Degree> degrees = computeDegree(builder,"values","numvals");
        System.out.println("Execution time (ms) ["+numV+"|"+numE+"]: " + w.elapsed(TimeUnit.MILLISECONDS));
        assertNotNull(degrees);
        assertEquals(numV,degrees.size());
        int totalCount = 0;
        for (Map.Entry<Long,Degree> entry : degrees.entries()) {
            Degree degree = entry.getValue();
            assertEquals(degree.in+degree.out,degree.both);
            Vertex v = tx.v(entry.getKey().longValue());
            int count = v.value("uid");
            assertEquals(count,degree.out);
            int numvals = v.value("numvals");
            assertEquals(numvals,degree.prop);
            totalCount+= degree.both;
        }
        assertEquals(numV*(numV+1),totalCount);
    }

    public static OLAPResult<Degree> computeDegree(final OLAPJobBuilder<Degree> builder, final String aggregatePropKey,
                                                   final String checkPropKey) throws Exception {
        builder.setInitializer(new StateInitializer<Degree>() {
            @Override
            public Degree initialState() {
                return new Degree();
            }
        });
        builder.setNumProcessingThreads(2);
        builder.setStateKey("degree");
        builder.setJob(new OLAPJob() {
            @Override
            public Degree process(TitanVertex vertex) {
                Degree d = vertex.value("all");
                if (d==null) d = new Degree();
                Degree p = vertex.value(aggregatePropKey);
                if (checkPropKey!=null) assertNotNull(vertex.value(checkPropKey));
                if (p!=null) d.add(p);
                return d;
            }
        });
        builder.addQuery().setName("all").edges(new Gather<Degree, Degree>() {
                                                    @Override
                                                    public Degree apply(Degree state, TitanEdge edge, Direction dir) {
                                                        return new Degree(dir == Direction.IN ? 1 : 0, dir == Direction.OUT ? 1 : 0, 0);
                                                    }
                                                }, new Combiner<Degree>() {
                                                    @Override
                                                    public Degree combine(Degree m1, Degree m2) {
                                                        m1.add(m2);
                                                        return m1;
                                                    }
                                                }
        );
        builder.addQuery().keys(aggregatePropKey).properties(new Function<TitanVertexProperty, Degree>() {
                                                         @Nullable
                                                         @Override
                                                         public Degree apply(@Nullable TitanVertexProperty titanProperty) {
                                                             return new Degree(0,0,1);
                                                         }
                                                     }, new Combiner<Degree>() {
                                                         @Override
                                                         public Degree combine(Degree m1, Degree m2) {
                                                             m1.add(m2);
                                                             return m1;
                                                         }
                                                     });
        if (checkPropKey!=null) builder.addQuery().keys(checkPropKey).properties();
        return builder.execute().get(200, TimeUnit.SECONDS);
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
        v.singleProperty("distance",distance);
        if (distance<diameter) {
            Vertex previous = null;
            for (int i=0;i<branch;i++) {
                Vertex u = tx.addVertex();
                u.addEdge("likes",v);
                if (previous!=null) u.addEdge("knows",previous);
                previous=u;
                expand(u,distance+1,diameter,branch);
            }
        }
    }

    @Test
    public void pageRank() {
        mgmt.makePropertyKey("distance").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("likes").multiplicity(Multiplicity.MULTI).make();
        finishSchema();
        final int branch = 5;
        final int diameter = 5;
        final double alpha = 0.85d;
        int numV = (int)((Math.pow(branch,diameter+1)-1)/(branch-1));
        Vertex v = tx.addVertex();
        expand(v,0,diameter,branch);
        clopen();
        assertCount(numV,tx.V());
        newTx();

        //Precompute correct PR results:
        double[] correctPR = new double[diameter+1];
        for (int i=diameter;i>=0;i--) {
            double pr = 1.0/numV*(1-alpha);
            if (i<diameter) pr+= alpha*branch*correctPR[i+1];
            correctPR[i]=pr;
        }

        Stopwatch w = new Stopwatch().start();
        OLAPResult<PageRank> ranks = computePageRank(graph,alpha,1,numV,"likes");
        System.out.println(String.format("Computing PR on graph with %s vertices took: %s ms",numV,w.elapsed(TimeUnit.MILLISECONDS)));
        double totalPr = 0.0;
        for (Map.Entry<Long,PageRank> entry : ranks.entries()) {
            Vertex u = tx.v(entry.getKey());
            int distance = u.<Integer>value("distance");
            double pr = entry.getValue().getPr();
            assertEquals(correctPR[distance],pr,EPSILON);
//            System.out.println(distance+" -> "+pr);
            totalPr+=pr;
        }
//        System.out.println("Total PR: " + totalPr);

    }

    private static final double PR_TERMINATION_THRESHOLD = 0.01;

    private OLAPResult<PageRank> computePageRank(final StandardTitanGraph g, final double alpha, final int numThreads,
                                               final int numVertices, final String... labels) {
        //Initializing
        OLAPJobBuilder<PageRank> builder = getOLAPBuilder(graph,PageRank.class);
        builder.setNumProcessingThreads(numThreads);
        builder.setStateKey("pageRank");
        builder.setNumVertices(numVertices);
        OLAPQueryBuilder<PageRank,?,?> query = builder.addQuery().setName("degree").direction(Direction.OUT);
        if (labels!=null && labels.length>0) {
            query.labels(labels);
        }
        query.edges(new Gather<PageRank, Long>() {
            @Override
            public Long apply(PageRank state, TitanEdge edge, Direction dir) {
                return 1l;
            }
        },new Combiner<Long>() {
            @Override
            public Long combine(Long m1, Long m2) {
                return m1+m2;
            }
        });
        builder.setJob(new OLAPJob() {
            @Override
            public PageRank process(TitanVertex vertex) {
                Long degree = vertex.value("degree");
                return new PageRank(degree!=null?degree:0,1.0d/numVertices);
            }
        });
        OLAPResult<PageRank> ranks;
        try {
            ranks = builder.execute().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertEquals(numVertices,ranks.size());
        double totalDelta;
        int iteration = 0;
        do {
            builder = getOLAPBuilder(graph,PageRank.class);
            builder.setNumProcessingThreads(numThreads);
            builder.setStateKey("pageRank");
            builder.setInitialState(ranks);
            query = builder.addQuery().setName("energy").direction(Direction.IN);
            if (labels!=null && labels.length>0) {
                query.labels(labels);
            }
            query.edges(new Gather<PageRank, Double>() {
                @Override
                public Double apply(PageRank state, TitanEdge edge, Direction dir) {
                    return state.getPrFlow();
                }
            },new Combiner<Double>() {
                @Override
                public Double combine(Double m1, Double m2) {
                    return m1+m2;
                }
            });
            builder.setJob(new OLAPJob() {
                @Override
                public PageRank process(TitanVertex vertex) {
                    PageRank pr = vertex.<PageRank>value("pageRank");
                    Double energy = vertex.value("energy");
                    if (energy==null) energy=0.0;
                    pr.setPr(energy, alpha, numVertices);
                    return pr;
                }
            });
            Stopwatch w = new Stopwatch().start();
            try {
                ranks = builder.execute().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            iteration++;
            assertEquals(numVertices,ranks.size());
            totalDelta = 0.0;
            for (PageRank pr : ranks.values()) {
                totalDelta+=pr.completeIteration();
            }
            System.out.println(String.format("Completed iteration [%s] in time %s ms with delta PR=%s",iteration,w.elapsed(TimeUnit.MILLISECONDS),totalDelta));
        } while (totalDelta>PR_TERMINATION_THRESHOLD);
        return ranks;
    }

    public static class PageRank {

        private double edgeCount;
        private double oldPR;
        private double newPR;

        public PageRank(long edgeCount, double initialPR) {
            Preconditions.checkArgument(edgeCount>=0 && initialPR>=0);
            this.edgeCount=edgeCount;
            this.oldPR = initialPR;
            this.newPR = -1.0;
        }

        public void setPr(double energy, double alpha, long numVertices) {
            newPR = alpha * energy + (1.0 - alpha) / numVertices;
        }

        public double getPrFlow() {
            Preconditions.checkArgument(oldPR>=0 && edgeCount>0.0);
            return oldPR/edgeCount;
        }

        public double getPr() {
            return oldPR;
        }

        public double completeIteration() {
            double delta = Math.abs(oldPR-newPR);
            oldPR=newPR;
            newPR=0.0;
            return delta;
        }

    }

    @Test
    public void singleSourceShortestPaths() throws Exception {
        PropertyKey distance = mgmt.makePropertyKey("distance").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("connect").signature(distance).multiplicity(Multiplicity.MULTI).make();
        finishSchema();

        int maxDepth = 16;
        int maxBranch = 5;
        Vertex vertex = tx.addVertex();
        //Grow a star-shaped graph around vertex which will be the single-source for this shortest path computation
        final int numV = growVertex(vertex,0,maxDepth, maxBranch);
        final int numE = numV-1;
        assertCount(numV,tx.V());
        assertCount(numE,tx.E());

        clopen();

        OLAPResult<Integer> distances = null;
        final AtomicBoolean done = new AtomicBoolean(false);

        while (!done.get()) {
            done.set(true);
            OLAPJobBuilder<Integer> builder = getOLAPBuilder(graph,Integer.class);

            if (distances==null) { //First iteration
                builder.setInitializer(new StateInitializer<Integer>() {
                    @Override
                    public Integer initialState() {
                        return Integer.MAX_VALUE;
                    }
                });
                builder.setInitialState(ImmutableMap.of(getId(vertex),0));
            } else {
                builder.setInitialState(distances);
            }
            builder.setNumProcessingThreads(2);
            builder.setStateKey("dist");
            builder.setJob(new OLAPJob<Integer>() {
                @Override
                public Integer process(TitanVertex vertex) {
                    Integer d = vertex.value("dist");
                    assertNotNull(d);
                    Integer c = vertex.value("connect");
                    int result = c==null?d:Math.min(c,d);
                    if (result<d) done.set(false);
                    return result;
                }
            });
            builder.addQuery().labels("connect").direction(Direction.BOTH).edges(
             new Gather<Integer, Integer>() {
                 @Override
                 public Integer apply(Integer state, TitanEdge edge, Direction dir) {
                     assertNotNull(state);
                     if (state.intValue() == Integer.MAX_VALUE)
                         return state;
                     else
                         return state + edge.<Integer>value("distance");
                 }
             }, new Combiner<Integer>() {
                 @Override
                 public Integer combine(Integer m1, Integer m2) {
                     return Math.min(m1, m2);
                 }
             }
            );
            Stopwatch w = new Stopwatch().start();
            distances = builder.execute().get(200, TimeUnit.SECONDS);
            System.out.println("Execution time (ms) [" + numV + "|" + numE + "]: " + w.elapsed(TimeUnit.MILLISECONDS));
            assertEquals(numV,distances.size());
        }
        for (Map.Entry<Long,Integer> entry : distances.entries()) {
            int dist = entry.getValue();
            assertTrue("Invalid distance: " + dist,dist >= 0 && dist < Integer.MAX_VALUE);
            Vertex v = tx.v(entry.getKey().longValue());
            assertEquals(v.<Integer>value("distance").intValue(),dist);
        }
    }

    private int growVertex(Vertex vertex, int depth, int maxDepth, int maxBranch) {
        vertex.singleProperty("distance",depth);
        int total=1;
        if (depth>=maxDepth) return total;

        for (int i=0;i<random.nextInt(maxBranch)+1;i++) {
            int dist = random.nextInt(3)+1;
            Vertex n = tx.addVertex();
            n.addEdge("connect",vertex, "distance",dist);
            total+=growVertex(n,depth+dist,maxDepth,maxBranch);
        }
        return total;
    }


}
