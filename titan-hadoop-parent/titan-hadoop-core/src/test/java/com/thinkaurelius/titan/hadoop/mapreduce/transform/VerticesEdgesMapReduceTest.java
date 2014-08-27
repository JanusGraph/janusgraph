package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.hadoop.*;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesEdgesMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new VerticesEdgesMapReduce.Map());
        mapReduceDriver.setReducer(new VerticesEdgesMapReduce.Reduce());
    }

    public void testOutCreatedTraversalWithPaths() throws Exception {
        Configuration config = VerticesEdgesMapReduce.createConfiguration(Direction.OUT, "created");
        config.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        for (FaunusVertex vertex : graph.values()) {

            for (Edge edge : vertex.getEdges(Direction.BOTH, "knows")) {
                assertEquals(((StandardFaunusEdge) edge).pathCount(), 0);
            }
            for (Edge edge : vertex.getEdges(Direction.OUT, "created")) {
                //System.out.println(vertex + " " + edge);
                assertEquals(edge + " path count", ((StandardFaunusEdge) edge).pathCount(), 1);
                assertEquals(((StandardFaunusEdge) edge).getPaths().get(0).size(), 2);
                assertEquals(((StandardFaunusEdge) edge).getPaths().get(0).get(0).getId(), edge.getVertex(Direction.OUT).getId());
                assertEquals(((StandardFaunusEdge) edge).getPaths().get(0).get(1).getId(), ((StandardFaunusEdge) edge).getLongId());
            }

            for (Edge edge : vertex.getEdges(Direction.IN, "created")) {
                //System.out.println(vertex + " " + edge);
                assertEquals(((StandardFaunusEdge) edge).pathCount(), 1);
                assertEquals(((StandardFaunusEdge) edge).getPaths().get(0).size(), 2);
                assertEquals(((StandardFaunusEdge) edge).getPaths().get(0).get(0).getId(), edge.getVertex(Direction.OUT).getId());
                assertEquals(((StandardFaunusEdge) edge).getPaths().get(0).get(1).getId(), ((StandardFaunusEdge) edge).getLongId());
            }
        }

        noPaths(graph, Vertex.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, VerticesEdgesMapReduce.Counters.EDGES_TRAVERSED), 4);
    }

    public void testOutAllTraversalWithPaths() throws Exception {
        Configuration config = VerticesEdgesMapReduce.createConfiguration(Direction.OUT);
        config.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        for (FaunusVertex vertex : graph.values()) {
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((StandardFaunusEdge) edge).pathCount(), 1);
                assertEquals(((StandardFaunusEdge) edge).getPaths().get(0).size(), 2);
                assertEquals(((StandardFaunusEdge) edge).getPaths().get(0).get(0).getId(), edge.getVertex(Direction.OUT).getId());
                assertEquals(((StandardFaunusEdge) edge).getPaths().get(0).get(1).getId(), ((StandardFaunusEdge) edge).getLongId());
            }
        }

        noPaths(graph, Vertex.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, VerticesEdgesMapReduce.Counters.EDGES_TRAVERSED), 6);
    }

    public void testOutAllTraversal() throws Exception {
        Configuration config = VerticesEdgesMapReduce.createConfiguration(Direction.OUT);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        for (FaunusVertex vertex : graph.values()) {
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((StandardFaunusEdge) edge).pathCount(), 1);
                try {
                    ((StandardFaunusEdge) edge).getPaths();
                    assertTrue(false);
                } catch (IllegalStateException e) {
                    assertTrue(true);
                }
            }
        }

        noPaths(graph, Vertex.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, VerticesEdgesMapReduce.Counters.EDGES_TRAVERSED), 6);
    }

    public void testBothCreatedTraversal() throws Exception {
        Configuration config = VerticesEdgesMapReduce.createConfiguration(Direction.BOTH, "created");
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        for (FaunusVertex vertex : graph.values()) {
            for (Edge edge : vertex.getEdges(Direction.BOTH, "created")) {
                assertEquals(((StandardFaunusEdge) edge).pathCount(), 2);
            }
        }

        noPaths(graph, Vertex.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, VerticesEdgesMapReduce.Counters.EDGES_TRAVERSED), 8);
    }
}
