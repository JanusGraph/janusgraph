package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
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
public class VerticesVerticesMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new VerticesVerticesMapReduce.Map());
        mapReduceDriver.setReducer(new VerticesVerticesMapReduce.Reduce());
    }

    public void testKnowsCreatedTraversal() throws IOException {
        Configuration config = new Configuration();
        config.set(VerticesVerticesMapReduce.DIRECTION, Direction.OUT.name());
        config.setStrings(VerticesVerticesMapReduce.LABELS, "created");

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH), Vertex.class, false), mapReduceDriver);
        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 0);
        assertEquals(results.get(2l).pathCount(), 0);
        assertEquals(results.get(3l).pathCount(), 3);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 1);
        assertEquals(results.get(6l).pathCount(), 0);
    }

    public void testKnowsCreatedTraversal2() throws IOException {
        Configuration config = new Configuration();
        config.set(VerticesVerticesMapReduce.DIRECTION, Direction.OUT.name());
        config.setStrings(VerticesVerticesMapReduce.LABELS, "created");
        config.setBoolean(FaunusCompiler.PATH_ENABLED, false);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(ExampleGraph.TINKERGRAPH), Vertex.class), mapReduceDriver);
        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 0);
        assertEquals(results.get(2l).pathCount(), 0);
        assertEquals(results.get(3l).pathCount(), 3);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 1);
        assertEquals(results.get(6l).pathCount(), 0);
    }
}
