package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new VertexMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testVerticesWith() throws Exception {
        Configuration config = VertexMap.createConfiguration(1, 2, 2346);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), mapReduceDriver);
        assertTrue(graph.get(1l).hasPaths());
        assertTrue(graph.get(2l).hasPaths());
        assertFalse(graph.get(3l).hasPaths());
        assertFalse(graph.get(4l).hasPaths());
        assertFalse(graph.get(5l).hasPaths());
        assertFalse(graph.get(6l).hasPaths());

        assertEquals(graph.get(1l).pathCount(), 1l);
        assertEquals(graph.get(2l).pathCount(), 1l);
        assertEquals(graph.get(3l).pathCount(), 0l);
        assertEquals(graph.get(4l).pathCount(), 0l);
        assertEquals(graph.get(5l).pathCount(), 0l);
        assertEquals(graph.get(6l).pathCount(), 0l);

        assertEquals(mapReduceDriver.getCounters().findCounter(VertexMap.Counters.VERTICES_PROCESSED).getValue(), 2);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testVerticesWithPaths() throws Exception {
        Configuration config = VertexMap.createConfiguration(1, 2, 2346, 2345, 3333, 1, 1, 2);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), mapReduceDriver);
        assertTrue(graph.get(1l).hasPaths());
        assertTrue(graph.get(2l).hasPaths());
        assertFalse(graph.get(3l).hasPaths());
        assertFalse(graph.get(4l).hasPaths());
        assertFalse(graph.get(5l).hasPaths());
        assertFalse(graph.get(6l).hasPaths());

        assertEquals(graph.get(1l).pathCount(), 1l);
        assertEquals(graph.get(2l).pathCount(), 1l);
        assertEquals(graph.get(3l).pathCount(), 0l);
        assertEquals(graph.get(4l).pathCount(), 0l);
        assertEquals(graph.get(5l).pathCount(), 0l);
        assertEquals(graph.get(6l).pathCount(), 0l);

        assertEquals(graph.get(1l).getPaths().size(), 1l);
        assertEquals(graph.get(2l).getPaths().size(), 1l);
        assertEquals(graph.get(3l).getPaths().size(), 0l);
        assertEquals(graph.get(4l).getPaths().size(), 0l);
        assertEquals(graph.get(5l).getPaths().size(), 0l);
        assertEquals(graph.get(6l).getPaths().size(), 0l);

        assertEquals(graph.get(1l).getPaths().get(0).size(), 1l);
        assertEquals(graph.get(2l).getPaths().get(0).size(), 1l);
        assertEquals(graph.get(1l).getPaths().get(0).get(0).getId(), 1l);
        assertEquals(graph.get(2l).getPaths().get(0).get(0).getId(), 2l);

        assertEquals(mapReduceDriver.getCounters().findCounter(VertexMap.Counters.VERTICES_PROCESSED).getValue(), 2);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

}
