package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyFilterMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new PropertyFilterMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testVerticesOnName() throws IOException {
        Configuration config = new Configuration();
        config.setClass(PropertyFilterMap.CLASS, Vertex.class, Element.class);
        config.set(PropertyFilterMap.KEY, "name");
        config.setClass(PropertyFilterMap.VALUE_CLASS, String.class, String.class);
        config.setStrings(PropertyFilterMap.VALUES, "marko", "vadas");
        config.set(PropertyFilterMap.COMPARE, Query.Compare.EQUAL.name());
        config.setBoolean(PropertyFilterMap.NULL_WILDCARD, false);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH), Vertex.class), mapReduceDriver);

        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 1);
        assertEquals(results.get(2l).pathCount(), 1);
        assertEquals(results.get(3l).pathCount(), 0);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 0);
        assertEquals(results.get(6l).pathCount(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.VERTICES_FILTERED).getValue(), 4);
        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.EDGES_FILTERED).getValue(), 0);

        identicalStructure(results, ExampleGraph.TINKERGRAPH);
    }

    public void testEdgesOnWeight() throws IOException {
        Configuration config = new Configuration();
        config.setClass(PropertyFilterMap.CLASS, Edge.class, Element.class);
        config.set(PropertyFilterMap.KEY, "weight");
        config.setClass(PropertyFilterMap.VALUE_CLASS, Float.class, Float.class);
        config.setFloat(PropertyFilterMap.VALUES, 0.2f);
        config.set(PropertyFilterMap.COMPARE, Query.Compare.EQUAL.name());
        config.setBoolean(PropertyFilterMap.NULL_WILDCARD, false);


        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH), Edge.class), mapReduceDriver);

        assertEquals(results.size(), 6);
        int counter = 0;
        for (FaunusVertex vertex : results.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                if (((FaunusEdge) edge).hasPaths()) {
                    counter = counter + ((FaunusEdge) edge).pathCount();
                    assertEquals(edge.getProperty("weight"), 0.2d);
                }
            }
        }
        assertEquals(counter, 2);

        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.VERTICES_FILTERED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.EDGES_FILTERED).getValue(), 10);

        identicalStructure(results, ExampleGraph.TINKERGRAPH);
    }
}
