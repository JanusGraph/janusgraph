package com.thinkaurelius.titan.olap;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.olap.*;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class OLAPTest extends TitanGraphBaseTest {

    private static final double EPSILON = 0.00001;

    protected abstract <S> OLAPJobBuilder<S> getOLAPBuilder(StandardTitanGraph graph, Class<S> clazz);

    private static final Random random = new Random();

    @Test
    public void degreeCount() throws Exception {
        mgmt.makePropertyKey("uid").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makePropertyKey("values").cardinality(Cardinality.LIST).dataType(Integer.class).make();
        mgmt.makePropertyKey("numvals").dataType(Integer.class).make();
        finishSchema();
        int numV = 300;
        int numE = 0;
        TitanVertex[] vs = new TitanVertex[numV];
        for (int i=0;i<numV;i++) {
            vs[i] = tx.addVertex();
            vs[i].setProperty("uid",i+1);
            int numVals = random.nextInt(5)+1;
            vs[i].setProperty("numvals",numVals);
            for (int j=0;j<numVals;j++) {
                vs[i].addProperty("values",random.nextInt(100));
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
            Vertex v = tx.getVertex(entry.getKey().longValue());
            int count = v.getProperty("uid");
            assertEquals(count,degree.out);
            int numvals = v.getProperty("numvals");
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
                Degree d = vertex.getProperty("all");
                if (d==null) d = new Degree();
                Degree p = vertex.getProperty(aggregatePropKey);
                if (checkPropKey!=null) assertNotNull(vertex.getProperty(checkPropKey));
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
        builder.addQuery().keys(aggregatePropKey).properties(new Function<TitanProperty, Degree>() {
                                                         @Nullable
                                                         @Override
                                                         public Degree apply(@Nullable TitanProperty titanProperty) {
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


    private void expand(TitanVertex v, final int distance, final int diameter, final int branch) {
        v.setProperty("distance",distance);
        if (distance<diameter) {
            TitanVertex previous = null;
            for (int i=0;i<branch;i++) {
                TitanVertex u = tx.addVertex();
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
        TitanVertex v = tx.addVertex();
        expand(v,0,diameter,branch);
        clopen();
        assertEquals(numV,Iterables.size(tx.getVertices()));
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
            Vertex u = tx.getVertex(entry.getKey());
            int distance = u.<Integer>getProperty("distance");
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
                Long degree = vertex.<Long>getProperty("degree");
                return new PageRank(degree==null?0:degree,1.0d/numVertices);
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
                    PageRank pr = vertex.<PageRank>getProperty("pageRank");
                    Double energy = vertex.getProperty("energy");
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
        TitanVertex vertex = tx.addVertex();
        //Grow a star-shaped graph around vertex which will be the single-source for this shortest path computation
        final int numV = growVertex(vertex,0,maxDepth, maxBranch);
        final int numE = numV-1;
        assertEquals(numV,Iterables.size(tx.getVertices()));
        assertEquals(numE,Iterables.size(tx.getEdges()));

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
                builder.setInitialState(ImmutableMap.of(vertex.getID(),0));
            } else {
                builder.setInitialState(distances);
            }
            builder.setNumProcessingThreads(2);
            builder.setStateKey("dist");
            builder.setJob(new OLAPJob<Integer>() {
                @Override
                public Integer process(TitanVertex vertex) {
                    Integer d = vertex.getProperty("dist");
                    assertNotNull(d);
                    Integer c = vertex.getProperty("connect");
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
                         return state + edge.<Integer>getProperty("distance");
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
            Vertex v = tx.getVertex(entry.getKey().longValue());
            assertEquals(v.<Integer>getProperty("distance").intValue(),dist);
        }
    }

    private int growVertex(TitanVertex vertex, int depth, int maxDepth, int maxBranch) {
        vertex.setProperty("distance",depth);
        int total=1;
        if (depth>=maxDepth) return total;

        for (int i=0;i<random.nextInt(maxBranch)+1;i++) {
            int dist = random.nextInt(3)+1;
            TitanVertex n = tx.addVertex();
            n.addEdge("connect",vertex).setProperty("distance",dist);
            total+=growVertex(n,depth+dist,maxDepth,maxBranch);
        }
        return total;
    }


}
