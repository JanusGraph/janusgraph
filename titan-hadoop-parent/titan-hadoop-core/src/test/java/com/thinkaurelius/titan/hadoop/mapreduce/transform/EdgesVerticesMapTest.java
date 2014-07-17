package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgesVerticesMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new EdgesVerticesMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testInVertices() throws Exception {
        Configuration config = EdgesVerticesMap.createConfiguration(Direction.IN);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Edge.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 3);
        assertEquals(graph.get(4l).pathCount(), 1);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, EdgesVerticesMap.Counters.IN_EDGES_PROCESSED), 6);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, EdgesVerticesMap.Counters.OUT_EDGES_PROCESSED), 0);

        noPaths(graph, Edge.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }

    public void testOutVertices() throws Exception {
        Configuration config = EdgesVerticesMap.createConfiguration(Direction.OUT);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Edge.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 2);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 1);


        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, EdgesVerticesMap.Counters.OUT_EDGES_PROCESSED), 6);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, EdgesVerticesMap.Counters.IN_EDGES_PROCESSED), 0);

        noPaths(graph, Edge.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }

    public void testBothVertices() throws Exception {
        Configuration config = EdgesVerticesMap.createConfiguration(Direction.BOTH);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Edge.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 3);
        assertEquals(graph.get(4l).pathCount(), 3);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 1);

        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, EdgesVerticesMap.Counters.OUT_EDGES_PROCESSED), 6);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, EdgesVerticesMap.Counters.IN_EDGES_PROCESSED), 6);

        noPaths(graph, Edge.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }

}
