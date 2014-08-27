package com.thinkaurelius.titan.hadoop.mapreduce.filter;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.Tokens;
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
public class DuplicateFilterMapTest extends BaseTest {


    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new DuplicateFilterMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testDedupVertices() throws Exception {
        Configuration config = DuplicateFilterMap.createConfiguration(Vertex.class);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, config);

        graph.get(1l).incrPath(3);
        graph.get(2l).incrPath(1);
        graph.get(3l).incrPath(2);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 2);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        graph = runWithGraph(graph, mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, DuplicateFilterMap.Counters.VERTICES_DEDUPED), 3);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, DuplicateFilterMap.Counters.EDGES_DEDUPED), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }


    public void testDedupVerticesWithPaths() throws Exception {
        Configuration config = DuplicateFilterMap.createConfiguration(Vertex.class);
        config.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, true);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, config);

        graph.get(1l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(2l)), false);
        graph.get(1l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(3l)), false);
        graph.get(1l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(4l)), false);
        graph.get(2l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(2l), new FaunusVertex.MicroVertex(1l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(3l), new FaunusVertex.MicroVertex(4l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(3l), new FaunusVertex.MicroVertex(5l)), false);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 3);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 2);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        graph = runWithGraph(graph, mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 0);
        assertEquals(graph.get(5l).pathCount(), 0);
        assertEquals(graph.get(6l).pathCount(), 0);

        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, DuplicateFilterMap.Counters.VERTICES_DEDUPED), 3);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, DuplicateFilterMap.Counters.EDGES_DEDUPED), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testDedupEdgesWithPaths() throws Exception {
        Configuration config = DuplicateFilterMap.createConfiguration(Edge.class);
        config.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, config);

        ((StandardFaunusEdge) graph.get(2l).getEdges(Direction.IN).iterator().next()).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(2l), new FaunusVertex.MicroVertex(1l)), false);
        ((StandardFaunusEdge) graph.get(2l).getEdges(Direction.IN).iterator().next()).addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(2l), new FaunusVertex.MicroVertex(1l)), false);

        assertEquals(graph.size(), 6);

        for (FaunusVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.IN)) {
                if (edge.getVertex(Direction.IN).getId().equals(2l)) {
                    assertEquals(((StandardFaunusEdge) edge).pathCount(), 2);
                } else {
                    assertEquals(((StandardFaunusEdge) edge).pathCount(), 0);
                }
            }
        }

        graph = runWithGraph(graph, mapReduceDriver);

        assertEquals(graph.size(), 6);

        for (FaunusVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.IN)) {
                if (edge.getVertex(Direction.IN).getId().equals(2l)) {
                    assertEquals(((StandardFaunusEdge) edge).pathCount(), 1);
                } else {
                    assertEquals(((StandardFaunusEdge) edge).pathCount(), 0);
                }
            }
        }

        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, DuplicateFilterMap.Counters.VERTICES_DEDUPED), 0);
        assertEquals(DEFAULT_COMPAT.getCounter(mapReduceDriver, DuplicateFilterMap.Counters.EDGES_DEDUPED), 1);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }
}
