package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
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

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new VerticesVerticesMapReduce.Map());
        mapReduceDriver.setCombiner(new VerticesVerticesMapReduce.Combiner());
        mapReduceDriver.setReducer(new VerticesVerticesMapReduce.Reduce());
    }

    public void testOutCreatedTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.OUT, "created");
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 3);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 4);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testOutAllTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.OUT, "knows", "created");
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 3);
        assertEquals(graph.get(4l).pathCount(), 1);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 6);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testInAllTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.IN);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 2);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 1);

        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 6);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testBothAllTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.BOTH);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 3);
        assertEquals(graph.get(4l).pathCount(), 3);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 1);

        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 12);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testBothCreatedTraversal() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.BOTH, "created");
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
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

        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 8);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testOutKnowsWithPaths() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.OUT, "knows");
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
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

        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 2);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testOutKnowsWithPathsOnlyMarko() throws Exception {
        Configuration config = VerticesVerticesMapReduce.createConfiguration(Direction.OUT, "created");
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, config);
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

        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesVerticesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 1);
        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }
}
