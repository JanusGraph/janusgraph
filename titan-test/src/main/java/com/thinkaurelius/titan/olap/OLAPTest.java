package com.thinkaurelius.titan.olap;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanVertexQuery;
import com.thinkaurelius.titan.core.olap.*;
import com.thinkaurelius.titan.graphdb.TitanGraphBaseTest;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class OLAPTest extends TitanGraphBaseTest {

    private static final double EPSILON = 0.00001;

    protected abstract <S extends State<S>> OLAPJobBuilder<S> getOLAPBuilder(StandardTitanGraph graph, Class<S> clazz);

    @Test
    public void degreeCount() throws Exception {
        mgmt.makePropertyKey("uid").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        finishSchema();
        int numV = 400;
        int numE = 0;
        TitanVertex[] vs = new TitanVertex[numV];
        for (int i=0;i<numV;i++) {
            vs[i] = tx.addVertex();
            vs[i].setProperty("uid",i+1);
        }
        Random random = new Random();
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

        OLAPJobBuilder<Degree> builder = getOLAPBuilder(graph,Degree.class);
        builder.setInitializer(new StateInitializer<Degree>() {
            @Override
            public Degree initialState() {
                return new Degree();
            }
        });
        builder.setNumProcessingThreads(1);
        builder.setStateKey("degree");
        builder.setJob(new OLAPJob() {
            @Override
            public void process(TitanVertex vertex) {
                Degree d = vertex.getProperty("degree");
                assertNotNull(d);
                d.in+=vertex.query().direction(Direction.IN).count();
                d.out+= Iterables.size(vertex.getEdges(Direction.OUT));
                d.both+= vertex.query().count();
            }
        });
        builder.addQuery().edges();
        Stopwatch w = new Stopwatch().start();
        OLAPResult<Degree> degrees = builder.execute().get(200, TimeUnit.SECONDS);
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
            totalCount+= degree.both;
        }
        assertEquals(numV*(numV+1),totalCount);
    }

    public class Degree implements State<Degree> {
        public int in;
        public int out;
        public int both;

        public Degree() {
            in=0;
            out=0;
            both=0;
        }

        @Override
        public Degree clone() {
            Degree d = new Degree();
            d.in=in;
            d.out=out;
            d.both=both;
            return d;
        }

        @Override
        public void merge(Degree other) {
            in+=other.in;
            out+=other.out;
            both+=other.both;
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
        if (labels==null || labels.length==0) {
            builder.addQuery().direction(Direction.OUT).edges();
        } else {
            builder.addQuery().direction(Direction.OUT).labels(labels).edges();
        }
        builder.setJob(new OLAPJob() {
            @Override
            public void process(TitanVertex vertex) {
                TitanVertexQuery query = vertex.query().direction(Direction.OUT);
                if (labels!=null && labels.length>0) query.labels(labels);
                long outDegree = query.count();
                vertex.setProperty("pageRank",new PageRank(outDegree,1.0d/numVertices));
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
            if (labels==null || labels.length==0) {
                builder.addQuery().direction(Direction.IN).edges();
            } else {
                builder.addQuery().direction(Direction.IN).labels(labels).edges();
            }
            builder.setJob(new OLAPJob() {
                @Override
                public void process(TitanVertex vertex) {
                    TitanVertexQuery query = vertex.query().direction(Direction.IN);
                    if (labels!=null && labels.length>0) query.labels(labels);
                    double total = 0.0;
                    for (Vertex v : query.vertices()) {
                        total+=v.<PageRank>getProperty("pageRank").getPrFlow();
                    }
                    vertex.<PageRank>getProperty("pageRank").setPr(total);
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
                totalDelta+=pr.completeIteration(alpha,numVertices);
            }
//            System.out.println(String.format("Completed iteration [%s] in time %s ms with delta PR=%s",iteration,w.elapsed(TimeUnit.MILLISECONDS),totalDelta));
        } while (totalDelta>PR_TERMINATION_THRESHOLD);
        return ranks;
    }

    public class PageRank implements State<PageRank> {

        private double edgeCount;
        private double oldPR;
        private double newPR;

        private PageRank(double edgeCount, double oldPR, double newPR) {
            this.edgeCount=edgeCount;
            this.oldPR=oldPR;
            this.newPR=newPR;
        }

        public PageRank(long edgeCount, double initialPR) {
            Preconditions.checkArgument(edgeCount>=0 && initialPR>=0);
            this.edgeCount=edgeCount;
            this.oldPR = initialPR;
            this.newPR = -1.0;
        }

        public void setPr(double pr) {
            newPR=pr;
        }

        public double getPrFlow() {
            Preconditions.checkArgument(oldPR>=0 && edgeCount>0.0);
            return oldPR/edgeCount;
        }

        public double getPr() {
            return oldPR;
        }

        public double completeIteration(double alpha, long numVertices) {
            newPR = alpha * newPR + (1.0 - alpha) / numVertices;
            double delta = Math.abs(oldPR-newPR);
            oldPR=newPR;
            newPR=0.0;
            return delta;
        }

        @Override
        public PageRank clone() {
            return new PageRank(edgeCount,oldPR,newPR);
        }

        @Override
        public void merge(PageRank other) {
            if (newPR==-1.0) { //Initial state => sum up degrees
                assert other.newPR==-1.0 && oldPR==other.oldPR;
                edgeCount+=other.edgeCount;
            } else {
                assert edgeCount==other.edgeCount && oldPR==other.oldPR;
                newPR+=other.newPR;
            }
        }

    }

}
