package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CyclicPathFilterMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new CyclicPathFilterMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testVertices() throws Exception {
        Configuration config = CyclicPathFilterMap.createConfiguration(Vertex.class);
        mapReduceDriver.withConfiguration(config);
        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        graph.get(1l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(2l)), false);
        graph.get(1l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(1l)), false);
        graph.get(1l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(1l)), false);

        assertEquals(graph.get(1l).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        graph = runWithGraph(graph, mapReduceDriver);

        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);


        assertEquals(mapReduceDriver.getCounters().findCounter(CyclicPathFilterMap.Counters.PATHS_FILTERED).getValue(), 2);

        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }

    public void testEdges() throws Exception {
        Configuration config = CyclicPathFilterMap.createConfiguration(Edge.class);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(((FaunusEdge) graph.get(1l).getEdges(Direction.OUT).iterator().next()).pathCount(), 0);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        ((FaunusEdge) graph.get(1l).getEdges(Direction.OUT).iterator().next()).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(2l)), false);
        ((FaunusEdge) graph.get(1l).getEdges(Direction.OUT).iterator().next()).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(1l)), false);
        ((FaunusEdge) graph.get(1l).getEdges(Direction.OUT).iterator().next()).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(1l)), false);

        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(((FaunusEdge) graph.get(1l).getEdges(Direction.OUT).iterator().next()).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        graph = runWithGraph(graph, mapReduceDriver);

        assertEquals(graph.get(1l).pathCount(), 0);
        assertEquals(((FaunusEdge) graph.get(1l).getEdges(Direction.OUT).iterator().next()).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 0);
        assertEquals(graph.get(3l).pathCount(), 0);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);


        assertEquals(mapReduceDriver.getCounters().findCounter(CyclicPathFilterMap.Counters.PATHS_FILTERED).getValue(), 2);

        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }
}
