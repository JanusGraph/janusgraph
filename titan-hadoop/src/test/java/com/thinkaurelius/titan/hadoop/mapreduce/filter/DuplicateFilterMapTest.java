package com.thinkaurelius.titan.hadoop.mapreduce.filter;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
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


    MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex>();
        mapReduceDriver.setMapper(new DuplicateFilterMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, HadoopVertex, NullWritable, HadoopVertex>());
    }

    public void testDedupVertices() throws Exception {
        Configuration config = DuplicateFilterMap.createConfiguration(Vertex.class);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, config);

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

        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.VERTICES_DEDUPED).getValue(), 3);
        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.EDGES_DEDUPED).getValue(), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }


    public void testDedupVerticesWithPaths() throws Exception {
        Configuration config = DuplicateFilterMap.createConfiguration(Vertex.class);
        config.setBoolean(Tokens.FAUNUS_PIPELINE_TRACK_PATHS, true);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, config);

        graph.get(1l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(1l), new HadoopVertex.MicroVertex(2l)), false);
        graph.get(1l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(1l), new HadoopVertex.MicroVertex(3l)), false);
        graph.get(1l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(1l), new HadoopVertex.MicroVertex(4l)), false);
        graph.get(2l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(2l), new HadoopVertex.MicroVertex(1l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(3l), new HadoopVertex.MicroVertex(4l)), false);
        graph.get(3l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(3l), new HadoopVertex.MicroVertex(5l)), false);

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

        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.VERTICES_DEDUPED).getValue(), 3);
        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.EDGES_DEDUPED).getValue(), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testDedupEdgesWithPaths() throws Exception {
        Configuration config = DuplicateFilterMap.createConfiguration(Edge.class);
        config.setBoolean(Tokens.FAUNUS_PIPELINE_TRACK_PATHS, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = generateGraph(ExampleGraph.TINKERGRAPH, config);

        ((HadoopEdge) graph.get(2l).getEdges(Direction.IN).iterator().next()).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(2l), new HadoopVertex.MicroVertex(1l)), false);
        ((HadoopEdge) graph.get(2l).getEdges(Direction.IN).iterator().next()).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(2l), new HadoopVertex.MicroVertex(1l)), false);

        assertEquals(graph.size(), 6);

        for (HadoopVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.IN)) {
                if (edge.getVertex(Direction.IN).getId().equals(2l)) {
                    assertEquals(((HadoopEdge) edge).pathCount(), 2);
                } else {
                    assertEquals(((HadoopEdge) edge).pathCount(), 0);
                }
            }
        }

        graph = runWithGraph(graph, mapReduceDriver);

        assertEquals(graph.size(), 6);

        for (HadoopVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.IN)) {
                if (edge.getVertex(Direction.IN).getId().equals(2l)) {
                    assertEquals(((HadoopEdge) edge).pathCount(), 1);
                } else {
                    assertEquals(((HadoopEdge) edge).pathCount(), 0);
                }
            }
        }


        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.VERTICES_DEDUPED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.EDGES_DEDUPED).getValue(), 1);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }
}
