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

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CommitVerticesMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new CommitVerticesMapReduce.Map());
        mapReduceDriver.setReducer(new CommitVerticesMapReduce.Reduce());
    }

    public void testKeepAllVertices() throws IOException {

        Configuration config = new Configuration();
        config.set(CommitVerticesMapReduce.ACTION, Tokens.Action.KEEP.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 1);
        assertEquals(results.get(2l).pathCount(), 1);
        assertEquals(results.get(3l).pathCount(), 1);
        assertEquals(results.get(4l).pathCount(), 1);
        assertEquals(results.get(5l).pathCount(), 1);
        assertEquals(results.get(6l).pathCount(), 1);


        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_DROPPED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_KEPT).getValue(), 6);

    }

    public void testDropAllVertices() throws IOException {

        Configuration config = new Configuration();
        config.set(CommitVerticesMapReduce.ACTION, Tokens.Action.DROP.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);
        assertEquals(results.size(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_DROPPED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_KEPT).getValue(), 0);

    }

    public void testKeepProjectVertices() throws IOException {

        Configuration config = new Configuration();
        config.set(CommitVerticesMapReduce.ACTION, Tokens.Action.KEEP.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);
        results.get(5l).startPath();
        results.get(3l).startPath();

        results = runWithGraph(results.values(), mapReduceDriver);
        assertEquals(results.size(), 2);
        assertEquals(results.get(5l).pathCount(), 1);
        assertEquals(results.get(3l).pathCount(), 1);

        assertFalse(results.get(5l).getEdges(Direction.BOTH).iterator().hasNext());
        assertFalse(results.get(3l).getEdges(Direction.BOTH).iterator().hasNext());

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_DROPPED).getValue(), 4);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_KEPT).getValue(), 2);

    }

    public void testDropProjectVertices() throws IOException {

        Configuration config = new Configuration();
        config.set(CommitVerticesMapReduce.ACTION, Tokens.Action.KEEP.name());

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);
        results.get(1l).startPath();
        results.get(2l).startPath();
        results.get(4l).startPath();
        results.get(6l).startPath();

        results = runWithGraph(results.values(), mapReduceDriver);
        assertEquals(results.size(), 4);
        assertEquals(results.get(1l).pathCount(), 1);
        assertEquals(results.get(2l).pathCount(), 1);
        assertEquals(results.get(4l).pathCount(), 1);
        assertEquals(results.get(6l).pathCount(), 1);

        for (FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(Direction.BOTH, "created").iterator().hasNext());
        }

        assertTrue(results.get(1l).getEdges(Direction.BOTH, "knows").iterator().hasNext());
        assertFalse(results.get(1l).getEdges(Direction.IN, "knows").iterator().hasNext());
        assertTrue(results.get(2l).getEdges(Direction.BOTH, "knows").iterator().hasNext());
        assertFalse(results.get(2l).getEdges(Direction.OUT, "knows").iterator().hasNext());
        assertTrue(results.get(4l).getEdges(Direction.BOTH, "knows").iterator().hasNext());
        assertFalse(results.get(4l).getEdges(Direction.OUT, "knows").iterator().hasNext());
        assertFalse(results.get(6l).getEdges(Direction.BOTH).iterator().hasNext());

        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_DROPPED).getValue(), 2);
        assertEquals(mapReduceDriver.getCounters().findCounter(CommitVerticesMapReduce.Counters.VERTICES_KEPT).getValue(), 4);

    }
}
