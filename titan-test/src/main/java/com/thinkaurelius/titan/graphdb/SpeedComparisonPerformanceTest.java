package com.thinkaurelius.titan.graphdb;

import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanVertexQuery;
import com.thinkaurelius.titan.core.VertexList;
import com.thinkaurelius.titan.testcategory.PerformanceTests;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Category({ PerformanceTests.class })
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
        graph.createKeyIndex("uid", Vertex.class);
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

    private static long time() {
        return System.currentTimeMillis();
    }

}
