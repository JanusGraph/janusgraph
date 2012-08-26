package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new LinkMapReduce.Map());
        mapReduceDriver.setReducer(new LinkMapReduce.Reduce());
    }

    public void testKnowsCreatedTraversal() throws IOException {

        Configuration config = new Configuration();
        config.set(LinkMapReduce.STEP, "blah");
        config.setInt("blah", 0);
        config.set(LinkMapReduce.DIRECTION, Direction.IN.name());
        config.set(LinkMapReduce.LABEL, "knowsCreated");

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedToyGraph(BaseTest.ExampleGraph.TINKERGRAPH);
        for (FaunusVertex vertex : results.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.BOTH);
        }
        results.get(3l).addPath(Arrays.asList(1l, 3l));
        results.get(5l).addPath(Arrays.asList(1l, 5l));

        results = runWithGraph(results.values(), mapReduceDriver);
        assertEquals(asList(results.get(1l).getEdges(OUT, "knowsCreated")).size(), 2);
        assertEquals(asList(results.get(1l).getEdges(BOTH)).size(), 2);
        assertFalse(results.get(2l).getEdges(BOTH).iterator().hasNext());
        assertEquals(asList(results.get(3l).getEdges(IN, "knowsCreated")).size(), 1);
        assertEquals(asList(results.get(3l).getEdges(BOTH)).size(), 1);
        assertFalse(results.get(4l).getEdges(BOTH).iterator().hasNext());
        assertEquals(asList(results.get(5l).getEdges(IN, "knowsCreated")).size(), 1);
        assertEquals(asList(results.get(5l).getEdges(BOTH)).size(), 1);
        assertFalse(results.get(6l).getEdges(BOTH).iterator().hasNext());

    }
}

