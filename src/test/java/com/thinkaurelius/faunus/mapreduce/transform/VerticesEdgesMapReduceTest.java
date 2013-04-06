package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
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
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

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
                assertEquals(((FaunusEdge) edge).pathCount(), 0);
            }
            for (Edge edge : vertex.getEdges(Direction.OUT, "created")) {
                //System.out.println(vertex + " " + edge);
                assertEquals(((FaunusEdge) edge).pathCount(), 1);
                assertEquals(((FaunusEdge) edge).getPaths().get(0).size(), 2);
                assertEquals(((FaunusEdge) edge).getPaths().get(0).get(0).getId(), edge.getVertex(Direction.OUT).getId());
                assertEquals(((FaunusEdge) edge).getPaths().get(0).get(1).getId(), edge.getId());
            }

            for (Edge edge : vertex.getEdges(Direction.IN, "created")) {
                //System.out.println(vertex + " " + edge);
                assertEquals(((FaunusEdge) edge).pathCount(), 1);
                assertEquals(((FaunusEdge) edge).getPaths().get(0).size(), 2);
                assertEquals(((FaunusEdge) edge).getPaths().get(0).get(0).getId(), edge.getVertex(Direction.OUT).getId());
                assertEquals(((FaunusEdge) edge).getPaths().get(0).get(1).getId(), edge.getId());
            }
        }

        noPaths(graph, Vertex.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesEdgesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 4);
    }

    public void testOutAllTraversalWithPaths() throws Exception {
        Configuration config = VerticesEdgesMapReduce.createConfiguration(Direction.OUT);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

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
                assertEquals(((FaunusEdge) edge).pathCount(), 1);
                assertEquals(((FaunusEdge) edge).getPaths().get(0).size(), 2);
                assertEquals(((FaunusEdge) edge).getPaths().get(0).get(0).getId(), edge.getVertex(Direction.OUT).getId());
                assertEquals(((FaunusEdge) edge).getPaths().get(0).get(1).getId(), edge.getId());
            }
        }

        noPaths(graph, Vertex.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesEdgesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 6);
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
                assertEquals(((FaunusEdge) edge).pathCount(), 1);
                try {
                    ((FaunusEdge) edge).getPaths();
                    assertTrue(false);
                } catch (IllegalStateException e) {
                    assertTrue(true);
                }
            }
        }

        noPaths(graph, Vertex.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesEdgesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 6);
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
                assertEquals(((FaunusEdge) edge).pathCount(), 2);
            }
        }

        noPaths(graph, Vertex.class);
        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
        assertEquals(mapReduceDriver.getCounters().findCounter(VerticesEdgesMapReduce.Counters.EDGES_TRAVERSED).getValue(), 8);
    }
}
