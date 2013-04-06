package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

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

    public void testVerticesFullStart() throws Exception {
        Configuration config = BackFilterMapReduce.createConfiguration(Vertex.class, 0);
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

    public void testVerticesBiasedStart() throws Exception {
        Configuration config = BackFilterMapReduce.createConfiguration(Vertex.class, 0);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        graph.get(1l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(1l)), false);
        graph.get(2l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(2l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(2l), new FaunusVertex.MicroVertex(3l)), false);
        graph.get(4l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(3l), new FaunusVertex.MicroVertex(4l)), false);
        graph.get(5l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(3l), new FaunusVertex.MicroVertex(5l)), false);

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

    public void testBackingUpToEdgesException() throws Exception {
        Configuration config = BackFilterMapReduce.createConfiguration(Vertex.class, 1);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        graph.get(1l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusEdge.MicroEdge(1l)), false);
        graph.get(2l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusEdge.MicroEdge(2l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(2l), new FaunusEdge.MicroEdge(3l)), false);
        graph.get(4l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(3l), new FaunusEdge.MicroEdge(4l)), false);
        graph.get(5l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(3l), new FaunusEdge.MicroEdge(5l)), false);

        try {
            graph = runWithGraph(graph, mapReduceDriver);
            assertFalse(true);
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    public void testBackingUpToVerticesFromEdges() throws Exception {
        Configuration config = BackFilterMapReduce.createConfiguration(Edge.class, 0);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        ((FaunusEdge) graph.get(1l).getEdges(Direction.OUT, "created").iterator().next()).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusEdge.MicroEdge(2l)), false);
        ((FaunusEdge) graph.get(6l).getEdges(Direction.OUT, "created").iterator().next()).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(6l), new FaunusEdge.MicroEdge(2l)), false);
        ((FaunusEdge) graph.get(6l).getEdges(Direction.OUT, "created").iterator().next()).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(2l), new FaunusEdge.MicroEdge(2l)), false);
        ((FaunusEdge) graph.get(6l).getEdges(Direction.OUT, "created").iterator().next()).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(2l), new FaunusEdge.MicroEdge(2l)), false);


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
