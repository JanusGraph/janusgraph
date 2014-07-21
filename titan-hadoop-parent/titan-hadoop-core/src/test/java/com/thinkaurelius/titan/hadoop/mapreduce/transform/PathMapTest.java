package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, Text, NullWritable, Text> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, Text, NullWritable, Text>();
        mapReduceDriver.setMapper(new PathMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, Text, NullWritable, Text>());
    }

    public void testPathsFromVertices() throws Exception {
        Configuration config = PathMap.createConfiguration(Vertex.class);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        graph.get(1l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(2l), new FaunusVertex.MicroVertex(1l)), false);
        graph.get(2l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(2l), new FaunusVertex.MicroVertex(2l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(3l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(3l)), false);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 2);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        final List<Pair<NullWritable, Text>> results = runWithGraphNoIndex(graph, mapReduceDriver);
        assertEquals(results.size(), 4);
        assertEquals(results.get(0).getSecond().toString(), "[v[2], v[1]]");
        assertEquals(results.get(1).getSecond().toString(), "[v[2], v[2]]");
        assertEquals(results.get(2).getSecond().toString(), "[v[1], v[3]]");
        assertEquals(results.get(3).getSecond().toString(), "[v[1], v[3]]");

        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, PathMap.Counters.VERTICES_PROCESSED), 3);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, PathMap.Counters.OUT_EDGES_PROCESSED), 0);

        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }

    public void testPathsAndGetException() throws Exception {
        Configuration config = PathMap.createConfiguration(Vertex.class);
        config.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, false);

        mapReduceDriver.withConfiguration(config);

        try {
            runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), mapReduceDriver);
            assertFalse(true);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }


    }
}
