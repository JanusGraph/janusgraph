package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.util.MicroEdge;
import com.thinkaurelius.faunus.util.MicroVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackFilterMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new BackFilterMapReduce.Map());
        mapReduceDriver.setCombiner(new BackFilterMapReduce.Combiner());
        mapReduceDriver.setReducer(new BackFilterMapReduce.Reduce());
    }

    public void testVerticesFullStart() throws IOException {
        Configuration config = new Configuration();
        config.setClass(BackFilterMapReduce.CLASS, Vertex.class, Element.class);
        config.setInt(BackFilterMapReduce.STEP, 0);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 1);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 1);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testVerticesBiasedStart() throws IOException {
        Configuration config = new Configuration();
        config.setClass(BackFilterMapReduce.CLASS, Vertex.class, Element.class);
        config.setInt(BackFilterMapReduce.STEP, 0);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        graph.get(1l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(1l)), false);
        graph.get(2l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(2l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new MicroVertex(2l), new MicroVertex(3l)), false);
        graph.get(4l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroVertex(4l)), false);
        graph.get(5l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroVertex(5l)), false);

        graph = runWithGraph(graph, mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 2);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 2);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testBackingUpToEdgesException() throws IOException {
        Configuration config = new Configuration();
        config.setClass(BackFilterMapReduce.CLASS, Vertex.class, Element.class);
        config.setInt(BackFilterMapReduce.STEP, 1);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        graph.get(1l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroEdge(1l)), false);
        graph.get(2l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroEdge(2l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new MicroVertex(2l), new MicroEdge(3l)), false);
        graph.get(4l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroEdge(4l)), false);
        graph.get(5l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroEdge(5l)), false);

        try {
            graph = runWithGraph(graph, mapReduceDriver);
            assertFalse(true);
        } catch (IOException e) {
            assertTrue(true);
        }
    }

    public void testBackingUpToVerticesFromEdges() throws IOException {
        Configuration config = new Configuration();
        config.setClass(BackFilterMapReduce.CLASS, Edge.class, Element.class);
        config.setInt(BackFilterMapReduce.STEP, 0);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        ((FaunusEdge) graph.get(1l).getEdges(Direction.OUT, "created").iterator().next()).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroEdge(2l)), false);
        ((FaunusEdge) graph.get(6l).getEdges(Direction.OUT, "created").iterator().next()).addPath((List) Arrays.asList(new MicroVertex(6l), new MicroEdge(2l)), false);
        ((FaunusEdge) graph.get(6l).getEdges(Direction.OUT, "created").iterator().next()).addPath((List) Arrays.asList(new MicroVertex(2l), new MicroEdge(2l)), false);
        ((FaunusEdge) graph.get(6l).getEdges(Direction.OUT, "created").iterator().next()).addPath((List) Arrays.asList(new MicroVertex(2l), new MicroEdge(2l)), false);


        graph = runWithGraph(graph, mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 2);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 1);

        for (Vertex vertex : graph.values()) {
            for (Edge e : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((FaunusEdge) e).pathCount(), 0);
            }
        }

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

}
