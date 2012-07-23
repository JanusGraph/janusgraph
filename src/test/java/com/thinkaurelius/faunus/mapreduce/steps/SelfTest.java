package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelfTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new Self.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testMap1() throws IOException {
        Configuration config = new Configuration();
        config.set(Self.ACTION, Tokens.Action.KEEP.name());
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final FaunusVertex vertex : results.values()) {
            assertEquals(asList(vertex.getEdges(Direction.BOTH)).size(), 0);
        }
    }

    public void testMap2() throws IOException {
        Configuration config = new Configuration();
        config.set(Self.ACTION, Tokens.Action.DROP.name());
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        Vertex vertex = results.get(1l);
        assertEquals(asList(vertex.getEdges(Direction.IN)).size(), 0);
        assertEquals(asList(vertex.getEdges(Direction.OUT)).size(), 3);

        vertex = results.get(2l);
        assertEquals(asList(vertex.getEdges(Direction.IN)).size(), 1);
        assertEquals(asList(vertex.getEdges(Direction.OUT)).size(), 0);

        vertex = results.get(3l);
        assertEquals(asList(vertex.getEdges(Direction.IN)).size(), 3);
        assertEquals(asList(vertex.getEdges(Direction.OUT)).size(), 0);

        vertex = results.get(4l);
        assertEquals(asList(vertex.getEdges(Direction.IN)).size(), 1);
        assertEquals(asList(vertex.getEdges(Direction.OUT)).size(), 2);

        vertex = results.get(5l);
        assertEquals(asList(vertex.getEdges(Direction.IN)).size(), 1);
        assertEquals(asList(vertex.getEdges(Direction.OUT)).size(), 0);

        vertex = results.get(6l);
        assertEquals(asList(vertex.getEdges(Direction.IN)).size(), 0);
        assertEquals(asList(vertex.getEdges(Direction.OUT)).size(), 1);

    }
    
    public void testMap3() throws IOException {
        Configuration config = new Configuration();
        config.set(Self.ACTION, Tokens.Action.DROP.name());
        mapReduceDriver.withConfiguration(config);
        Graph graph = new TinkerGraph();
        Vertex v1 = graph.addVertex(1);
        Vertex v2 = graph.addVertex(2);
        Vertex v3 = graph.addVertex(3);
        graph.addEdge(null, v1, v2, "knows");
        graph.addEdge(null, v1, v1, "knows");
        graph.addEdge(null, v3, v2, "created");
        graph.addEdge(null, v1, v3, "created");
        graph.addEdge(null, v3, v3, "created");

        Map<Long, FaunusVertex> results =  runWithGraph(graph, mapReduceDriver);
        Vertex vertex = results.get(1l);
        assertEquals(asList(vertex.getEdges(Direction.IN)).size(), 0);
        assertEquals(asList(vertex.getEdges(Direction.OUT)).size(), 2);

        vertex = results.get(2l);
        assertEquals(asList(vertex.getEdges(Direction.IN)).size(), 2);
        assertEquals(asList(vertex.getEdges(Direction.OUT)).size(), 0);

        vertex = results.get(3l);
        assertEquals(asList(vertex.getEdges(Direction.IN)).size(), 1);
        assertEquals(asList(vertex.getEdges(Direction.OUT)).size(), 1);
        
    }
}
