package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CommitEdgesMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new CommitEdgesMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testDropAllEdges() throws IOException {
        Configuration config = new Configuration();
        config.set(CommitEdgesMap.ACTION, Tokens.Action.DROP.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH), Edge.class, true), mapReduceDriver);
        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 0);
        assertEquals(results.get(2l).pathCount(), 0);
        assertEquals(results.get(3l).pathCount(), 0);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 0);
        assertEquals(results.get(6l).pathCount(), 0);

        for (FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(Direction.BOTH).iterator().hasNext());
        }

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitEdgesMap.Counters.EDGES_DROPPED).getValue(), 12);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitEdgesMap.Counters.EDGES_KEPT).getValue(), 0);
    }

    public void testKeepAllEdges() throws IOException {
        Configuration config = new Configuration();
        config.set(CommitEdgesMap.ACTION, Tokens.Action.KEEP.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH), Edge.class), mapReduceDriver);
        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 0);
        assertEquals(results.get(2l).pathCount(), 0);
        assertEquals(results.get(3l).pathCount(), 0);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 0);
        assertEquals(results.get(6l).pathCount(), 0);

        for (FaunusVertex vertex : results.values()) {
            assertTrue(vertex.getEdges(Direction.BOTH).iterator().hasNext());
        }

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitEdgesMap.Counters.EDGES_DROPPED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitEdgesMap.Counters.EDGES_KEPT).getValue(), 12);
    }

    public void testDropAllCreatedEdge() throws IOException {
        Configuration config = new Configuration();
        config.set(CommitEdgesMap.ACTION, Tokens.Action.DROP.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedGraph(BaseTest.ExampleGraph.TINKERGRAPH);
        for (FaunusVertex vertex : results.values()) {
            for (Edge edge : vertex.getEdges(Direction.BOTH, "created")) {
                ((FaunusEdge) edge).startPath();
            }
        }
        results = runWithGraph(results.values(), mapReduceDriver);
        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 0);
        assertEquals(results.get(2l).pathCount(), 0);
        assertEquals(results.get(3l).pathCount(), 0);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 0);
        assertEquals(results.get(6l).pathCount(), 0);

        int counter = 0;
        for (FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(Direction.BOTH, "created").iterator().hasNext());
            if (vertex.getEdges(Direction.BOTH, "knows").iterator().hasNext())
                counter++;
        }
        assertEquals(counter, 3);

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitEdgesMap.Counters.EDGES_DROPPED).getValue(), 8);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitEdgesMap.Counters.EDGES_KEPT).getValue(), 4);
    }

    public void testKeepAllCreatedEdge() throws IOException {
        Configuration config = new Configuration();
        config.set(CommitEdgesMap.ACTION, Tokens.Action.KEEP.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedGraph(BaseTest.ExampleGraph.TINKERGRAPH);
        for (FaunusVertex vertex : results.values()) {
            for (Edge edge : vertex.getEdges(Direction.BOTH, "created")) {
                ((FaunusEdge) edge).startPath();
            }
        }
        results = runWithGraph(results.values(), mapReduceDriver);
        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 0);
        assertEquals(results.get(2l).pathCount(), 0);
        assertEquals(results.get(3l).pathCount(), 0);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 0);
        assertEquals(results.get(6l).pathCount(), 0);

        int counter = 0;
        for (FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(Direction.BOTH, "knows").iterator().hasNext());
            if (vertex.getEdges(Direction.BOTH, "created").iterator().hasNext())
                counter++;
        }
        assertEquals(counter, 5);

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitEdgesMap.Counters.EDGES_DROPPED).getValue(), 4);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitEdgesMap.Counters.EDGES_KEPT).getValue(), 8);
    }

}
