package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.testcategory.PerformanceTests;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * TODO: Consolidate whatever is useful in here into other benchmark tests
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@Category({ PerformanceTests.class })
public abstract class SpeedComparisonBenchmark extends TitanGraphBaseTest {

    private static final int numVertices = 2000;
    private static final int edgesPerVertex = 400;

    @Test
    public void compare() {
        makeVertexIndexedUniqueKey("uid", Long.class);
        finishSchema();

        Vertex vertices[] = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) {
            vertices[i] = graph.addVertex(null);
            vertices[i].setProperty("uid", i);
        }
        for (int i = 0; i < numVertices; i++) {
            for (int j = 1; j <= edgesPerVertex; j++) {
                graph.addEdge(null, vertices[i], vertices[wrapAround(i + j, numVertices)], "connect");
            }
        }
        graph.commit();

        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                System.out.print("In Memory - ");
                retrieveNgh(true);
            } else {
                System.out.print("Direct - ");
                retrieveNgh(false);
            }
        }

    }


    private void retrieveNgh(boolean inMemory) {
        long time = time();
        Vertex vertices[] = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) vertices[i] = graph.getVertices("uid", i).iterator().next();
        time = time() - time;
        //System.out.println("Vertex retrieval: " + time);

        for (int t = 0; t < 4; t++) {
            time = time();
            for (int i = 0; i < numVertices; i++) {
                TitanVertexQuery q = ((TitanVertexQuery) vertices[i].query()).direction(Direction.OUT).labels("connect");
                if (inMemory) { //TODO: this has been disabled
                    for (Vertex v : q.vertices()) {
                        v.getId();
                    }
                } else {
                    VertexList vl = q.vertexIds();
                    for (int j = 0; j < vl.size(); j++) {
                        vl.get(j);
                    }
                }
            }
            time = time() - time;
            System.out.println("Ngh retrieval: " + time);
        }

        graph.commit();
    }

    @Test
    public void testIncrementalSpeed() {
        mgmt.makePropertyKey("payload").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        makeVertexIndexedUniqueKey("uid", Long.class);
        mgmt.makeEdgeLabel("activity").multiplicity(Multiplicity.MULTI).make();
        finishSchema();

        final int numV = 20;
        final int numA = 500;
        for (int i=1;i<=numV;i++) {
            TitanVertex v = graph.addVertex(null);
            v.setProperty("uid",i);
            for (int j=1;j<=numA;j++) {
                TitanVertex a = graph.addVertex(null);
                a.setProperty("payload", RandomStringUtils.randomAlphabetic(100));
                v.addEdge("activity",a);
            }
        }

        clopen();

        final int outer = 10;
        final int inner = 20;
        final int every = 5;
        assert numV % every == 0;

        long[][][] times = new long[outer][numV][inner];

        for (int u : new int[]{1,2}) {
            for (int o=0;o<outer;o++) {
                for (int i=0;i<numV;i++) {
                    if (i%every!=u) continue;
                    for (int j=0;j<inner;j++) {
                        long start = System.nanoTime();

                        Vertex v = graph.getVertices("uid",i).iterator().next();
                        assertEquals(numA,v.query().direction(Direction.OUT).labels("activity").count());

                        times[o][i][j]=(System.nanoTime()-start)/1000;
                        graph.commit();
                    }
                }
                clopen();
            }
        }
        for (int i=0;i<times.length;i++) {
            for (int j=0;j<times[i].length;j++) {
                if (times[i][j][0]==0) continue;
                System.out.println("v["+(j+1)+"]"+(j%every==2?"*":"")+":\t"+ Arrays.toString(times[i][j]));
            }
            System.out.println("------- Database Reopen -------------");
        }
    }


    private static long time() {
        return System.currentTimeMillis();
    }

}
