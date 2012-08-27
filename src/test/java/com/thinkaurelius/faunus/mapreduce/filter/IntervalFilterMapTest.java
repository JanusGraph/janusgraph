package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
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
public class IntervalFilterMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new IntervalFilterMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testVerticesOnAge() throws IOException {
        Configuration config = new Configuration();
        config.setClass(IntervalFilterMap.CLASS, Vertex.class, Element.class);
        config.setBoolean(IntervalFilterMap.NULL_WILDCARD, false);
        config.setClass(IntervalFilterMap.VALUE_CLASS, Number.class, Number.class);
        config.set(IntervalFilterMap.START_VALUE, "10");
        config.set(IntervalFilterMap.END_VALUE, "30");
        config.set(IntervalFilterMap.KEY, "age");

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedToyGraph(BaseTest.ExampleGraph.TINKERGRAPH);
        activate(results.values(), Vertex.class);

        results = runWithGraph(results.values(), mapReduceDriver);

        assertEquals(results.size(), 6);
        assertTrue(results.get(1l).isActive());
        assertTrue(results.get(2l).isActive());
        assertFalse(results.get(3l).isActive());
        assertFalse(results.get(4l).isActive());
        assertFalse(results.get(5l).isActive());
        assertFalse(results.get(6l).isActive());
    }

    public void testEdgesOnWeight() throws IOException {
        Configuration config = new Configuration();
        config.setClass(IntervalFilterMap.CLASS, Edge.class, Element.class);
        config.setBoolean(IntervalFilterMap.NULL_WILDCARD, false);
        config.setClass(IntervalFilterMap.VALUE_CLASS, Float.class, Float.class);
        config.set(IntervalFilterMap.START_VALUE, "0.3");
        config.set(IntervalFilterMap.END_VALUE, "0.45");
        config.set(IntervalFilterMap.KEY, "weight");

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedToyGraph(BaseTest.ExampleGraph.TINKERGRAPH);
        activate(results.values(), Edge.class);

        results = runWithGraph(results.values(), mapReduceDriver);
        assertEquals(results.size(), 6);

        int counter = 0;
        for (FaunusVertex vertex : results.values()) {
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                if (((FaunusEdge) edge).isActive()) {
                    counter++;
                    assertEquals(edge.getProperty("weight"), 0.4d);
                }
            }
        }
        assertEquals(counter, 4);
    }
}
