package com.thinkaurelius.titan.hadoop.mapreduce.filter;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyFilterMapTest extends BaseTest {

    MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex>();
        mapReduceDriver.setMapper(new PropertyFilterMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, HadoopVertex, NullWritable, HadoopVertex>());
    }

    public void testNullValue1() throws Exception {
        Configuration config = PropertyFilterMap.createConfiguration(Vertex.class, "age", Compare.EQUAL, new Object[]{null});
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.VERTICES_FILTERED).getValue(), 4);
        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.EDGES_FILTERED).getValue(), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testNullValue2() throws Exception {
        Configuration config = PropertyFilterMap.createConfiguration(Vertex.class, "age", Compare.EQUAL, null, 29);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.VERTICES_FILTERED).getValue(), 3);
        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.EDGES_FILTERED).getValue(), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testVerticesOnName() throws Exception {
        Configuration config = PropertyFilterMap.createConfiguration(Vertex.class, "name", Compare.EQUAL, "marko", "vadas");
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.VERTICES_FILTERED).getValue(), 4);
        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.EDGES_FILTERED).getValue(), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testEdgesOnWeight() throws Exception {
        Configuration config = PropertyFilterMap.createConfiguration(Edge.class, "weight", Compare.EQUAL, 0.2f);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Edge.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        long counter = 0;
        for (HadoopVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                if (((HadoopEdge) edge).hasPaths()) {
                    counter = counter + ((HadoopEdge) edge).pathCount();
                    assertEquals(edge.getProperty("weight"), 0.2d);
                }
            }
        }
        assertEquals(counter, 2);

        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.VERTICES_FILTERED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(PropertyFilterMap.Counters.EDGES_FILTERED).getValue(), 10);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }
}
