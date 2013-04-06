package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CommitVerticesMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new CommitVerticesMapReduce.Map());
        mapReduceDriver.setCombiner(new CommitVerticesMapReduce.Combiner());
        mapReduceDriver.setReducer(new CommitVerticesMapReduce.Reduce());
    }

    public void testKeepAllVertices() throws Exception {

        Configuration config = CommitVerticesMapReduce.createConfiguration(Tokens.Action.KEEP);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 1);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 1);


        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_DROPPED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_KEPT).getValue(), 6);

    }

    public void testDropAllVertices() throws Exception {
        Configuration config = CommitVerticesMapReduce.createConfiguration(Tokens.Action.DROP);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(results.size(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_DROPPED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_KEPT).getValue(), 0);

    }

    public void testKeepProjectVertices() throws Exception {
        Configuration config = CommitVerticesMapReduce.createConfiguration(Tokens.Action.KEEP);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);
        graph.get(5l).startPath();
        graph.get(3l).startPath();

        graph = runWithGraph(graph, mapReduceDriver);
        assertEquals(graph.size(), 2);
        assertEquals(graph.get(5l).pathCount(), 1);
        assertEquals(graph.get(3l).pathCount(), 1);

        assertFalse(graph.get(5l).getEdges(Direction.BOTH).iterator().hasNext());
        assertFalse(graph.get(3l).getEdges(Direction.BOTH).iterator().hasNext());

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_DROPPED).getValue(), 4);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_KEPT).getValue(), 2);

    }

    public void testDropProjectVertices() throws Exception {
        Configuration config = CommitVerticesMapReduce.createConfiguration(Tokens.Action.KEEP);
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);
        graph.get(1l).startPath();
        graph.get(2l).startPath();
        graph.get(4l).startPath();
        graph.get(6l).startPath();

        graph = runWithGraph(graph, mapReduceDriver);
        assertEquals(graph.size(), 4);
        assertEquals(graph.get(1l).pathCount(), 1);
        assertEquals(graph.get(2l).pathCount(), 1);
        assertEquals(graph.get(4l).pathCount(), 1);
        assertEquals(graph.get(6l).pathCount(), 1);

        for (FaunusVertex vertex : graph.values()) {
            assertFalse(vertex.getEdges(Direction.BOTH, "created").iterator().hasNext());
        }

        assertTrue(graph.get(1l).getEdges(Direction.BOTH, "knows").iterator().hasNext());
        assertFalse(graph.get(1l).getEdges(Direction.IN, "knows").iterator().hasNext());
        assertTrue(graph.get(2l).getEdges(Direction.BOTH, "knows").iterator().hasNext());
        assertFalse(graph.get(2l).getEdges(Direction.OUT, "knows").iterator().hasNext());
        assertTrue(graph.get(4l).getEdges(Direction.BOTH, "knows").iterator().hasNext());
        assertFalse(graph.get(4l).getEdges(Direction.OUT, "knows").iterator().hasNext());
        assertFalse(graph.get(6l).getEdges(Direction.BOTH).iterator().hasNext());

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_DROPPED).getValue(), 2);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_KEPT).getValue(), 4);

    }
}
