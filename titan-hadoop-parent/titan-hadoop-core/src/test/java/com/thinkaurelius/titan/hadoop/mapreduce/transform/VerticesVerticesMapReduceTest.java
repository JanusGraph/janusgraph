package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesVerticesMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, HadoopVertex, LongWritable, Holder, NullWritable, HadoopVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, LongWritable, Holder, NullWritable, HadoopVertex>();
        mapReduceDriver.setMapper(new VerticesVerticesMapReduce.Map());
        mapReduceDriver.setReducer(new VerticesVerticesMapReduce.Reduce());
    }

    public void testOutCreatedTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.OUT, "created");
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 3);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 0);

//        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 4);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED), 4);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testOutAllTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.OUT, "knows", "created");
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 3);
        assertEquals(graph.get(4l).pathCount(), 1);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 0);

//        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED), 6);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testInAllTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.IN);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 2);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 1);

//        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED), 6);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testBothAllTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.BOTH);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 3);
        assertEquals(graph.get(4l).pathCount(), 3);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 1);

//        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 12);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED), 12);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testBothCreatedTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.BOTH, "created");
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 3);
        assertEquals(graph.get(4l).pathCount(), 2);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 1);

        try {
            graph.get(1l).getPaths();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }

//        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 8);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED), 8);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testOutKnowsWithPaths() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.OUT, "knows");
        config.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 1);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(graph.get(2l).getPaths().size(), 1);
        assertEquals(graph.get(2l).getPaths().get(0).size(), 2);
        assertEquals(graph.get(2l).getPaths().get(0).get(0).getId(), 1l);
        assertEquals(graph.get(2l).getPaths().get(0).get(1).getId(), 2l);

        assertEquals(graph.get(4l).getPaths().size(), 1);
        assertEquals(graph.get(4l).getPaths().get(0).size(), 2);
        assertEquals(graph.get(4l).getPaths().get(0).get(0).getId(), 1l);
        assertEquals(graph.get(4l).getPaths().get(0).get(1).getId(), 4l);

//        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 2);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED), 2);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testOutKnowsWithPathsOnlyMarko() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.OUT, "created");
        config.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, config);
        graph.get(1l).startPath();
        graph = runWithGraph(graph, mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(graph.get(3l).getPaths().size(), 1);
        assertEquals(graph.get(3l).getPaths().get(0).size(), 2);
        assertEquals(graph.get(3l).getPaths().get(0).get(0).getId(), 1l);
        assertEquals(graph.get(3l).getPaths().get(0).get(1).getId(), 3l);

//        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 1);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED), 1);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }
}
