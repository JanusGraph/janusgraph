package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.util.MicroVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
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

    public void testDedupVertices() throws IOException {
        Configuration config = new Configuration();
        config.setClass(DuplicateFilterMap.CLASS, Vertex.class, Element.class);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedGraph(ExampleGraph.TINKERGRAPH);

        for(FaunusVertex v : results.values()) {
            v.enablePath(true);
        }

        results.get(1l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(2l)), false);
        results.get(1l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(3l)), false);
        results.get(1l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(4l)), false);
        results.get(2l).addPath((List) Arrays.asList(new MicroVertex(2l), new MicroVertex(1l)), false);
        results.get(3l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroVertex(4l)), false);
        results.get(3l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroVertex(5l)), false);

        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 3);
        assertEquals(results.get(2l).pathCount(), 1);
        assertEquals(results.get(3l).pathCount(), 2);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 0);
        assertEquals(results.get(6l).pathCount(), 0);

        results = runWithGraph(results.values(), mapReduceDriver);

        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 1);
        assertEquals(results.get(2l).pathCount(), 1);
        assertEquals(results.get(3l).pathCount(), 1);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 0);
        assertEquals(results.get(6l).pathCount(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.VERTICES_DEDUPED).getValue(), 3);
        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.EDGES_DEDUPED).getValue(), 0);

        identicalStructure(results, ExampleGraph.TINKERGRAPH);
    }

    public void testDedupEdges() throws IOException {
        Configuration config = new Configuration();
        config.setClass(DuplicateFilterMap.CLASS, Edge.class, Element.class);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedGraph(ExampleGraph.TINKERGRAPH);

        for(FaunusVertex v : results.values()) {
            for(Edge edge : v.getEdges(Direction.BOTH))
                ((FaunusEdge)edge).enablePath(true);
        }

        ((FaunusEdge) results.get(2l).getEdges(Direction.IN).iterator().next()).addPath((List) Arrays.asList(new MicroVertex(2l), new MicroVertex(1l)), false);
        ((FaunusEdge) results.get(2l).getEdges(Direction.IN).iterator().next()).addPath((List) Arrays.asList(new MicroVertex(2l), new MicroVertex(1l)), false);

        assertEquals(results.size(), 6);

        for (FaunusVertex vertex : results.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.IN)) {
                if (edge.getVertex(Direction.IN).getId().equals(2l)) {
                    assertEquals(((FaunusEdge) edge).pathCount(), 2);
                } else {
                    assertEquals(((FaunusEdge) edge).pathCount(), 0);
                }
            }
        }

        results = runWithGraph(results.values(), mapReduceDriver);

        assertEquals(results.size(), 6);

        for (FaunusVertex vertex : results.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.IN)) {
                if (edge.getVertex(Direction.IN).getId().equals(2l)) {
                    assertEquals(((FaunusEdge) edge).pathCount(), 1);
                } else {
                    assertEquals(((FaunusEdge) edge).pathCount(), 0);
                }
            }
        }


        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.VERTICES_DEDUPED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(DuplicateFilterMap.Counters.EDGES_DEDUPED).getValue(), 1);

        identicalStructure(results, ExampleGraph.TINKERGRAPH);
    }
}
