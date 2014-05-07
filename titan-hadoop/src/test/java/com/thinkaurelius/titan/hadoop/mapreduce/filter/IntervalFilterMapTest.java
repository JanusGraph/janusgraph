package com.thinkaurelius.titan.hadoop.mapreduce.filter;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
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
public class IntervalFilterMapTest extends BaseTest {

    MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex>();
        mapReduceDriver.setMapper(new IntervalFilterMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, HadoopVertex, NullWritable, HadoopVertex>());
    }

    public void testVerticesOnAge() throws Exception {
        Configuration config = IntervalFilterMap.createConfiguration(Vertex.class, "age", 10, 30);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(IntervalFilterMap.Counters.VERTICES_FILTERED).getValue(), 4);
        assertEquals(mapReduceDriver.getCounters().findCounter(IntervalFilterMap.Counters.EDGES_FILTERED).getValue(), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testEdgesOnWeight() throws Exception {
        Configuration config = IntervalFilterMap.createConfiguration(Edge.class, "weight", 0.3f, 0.45f);
        mapReduceDriver.withConfiguration(config);
        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Edge.class), mapReduceDriver);
        assertEquals(graph.size(), 6);

        long counter = 0;
        for (HadoopVertex vertex : graph.values()) {
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                if (((HadoopEdge) edge).hasPaths()) {
                    counter = ((HadoopEdge) edge).pathCount() + counter;
                    assertEquals(edge.getProperty("weight"), 0.4d);
                }
            }
        }
        assertEquals(counter, 4);

        assertEquals(mapReduceDriver.getCounters().findCounter(IntervalFilterMap.Counters.VERTICES_FILTERED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(IntervalFilterMap.Counters.EDGES_FILTERED).getValue(), 8);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }
}
