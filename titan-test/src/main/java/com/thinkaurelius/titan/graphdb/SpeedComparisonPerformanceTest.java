package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.core.TitanQuery;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public abstract class SpeedComparisonPerformanceTest extends TitanGraphTestCommon {

    private static final int numVertices = 2000;
    private static final int edgesPerVertex = 400;

    public SpeedComparisonPerformanceTest(Configuration config) {
        super(config);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        graphdb.createKeyIndex("uid", Vertex.class);
        Vertex vertices[] = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) {
            vertices[i] = graphdb.addVertex(null);
            vertices[i].setProperty("uid", i);
        }
        for (int i = 0; i < numVertices; i++) {
            for (int j = 1; j <= edgesPerVertex; j++) {
                graphdb.addEdge(null, vertices[i], vertices[wrapAround(i + j, numVertices)], "connect");
            }
        }
        graphdb.stopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    }

    @Test
    public void compare() {
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


    public void retrieveNgh(boolean inMemory) {
        long time = time();
        Vertex vertices[] = new TitanVertex[numVertices];
        for (int i = 0; i < numVertices; i++) vertices[i] = graphdb.getVertices("uid", i).iterator().next();
        time = time() - time;
        //System.out.println("Vertex retrieval: " + time);

        for (int t = 0; t < 4; t++) {
            time = time();
            for (int i = 0; i < numVertices; i++) {
                TitanQuery q = ((TitanQuery) vertices[i].query()).direction(Direction.OUT).labels("connect");
                if (inMemory) {
                    for (Vertex v : q.inMemory().vertices()) {
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

        graphdb.stopTransaction(TransactionalGraph.Conclusion.FAILURE);
    }

    private static long time() {
        return System.currentTimeMillis();
    }

}
