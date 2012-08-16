package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.derivations.LabelFilter;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LabelFilterTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new LabelFilter.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testDropCreated() throws IOException {
        Configuration config = new Configuration();
        config.set(LabelFilter.ACTION, Tokens.Action.DROP.name());
        config.setStrings(LabelFilter.LABELS, "created");
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(BOTH, "created").iterator().hasNext());
        }
        FaunusVertex vertex = results.get(1l);
        int counter = 0;
        for (Edge edge : vertex.getEdges(OUT)) {
            counter++;
            assertEquals(edge.getLabel(), "knows");
        }
        assertEquals(counter, 2);

    }

    public void testKeepCreated() throws IOException {
        Configuration config = new Configuration();
        config.set(LabelFilter.ACTION, Tokens.Action.KEEP.name());
        config.setStrings(LabelFilter.LABELS, "created");
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(BOTH, "knows").iterator().hasNext());
        }
        assertTrue(results.get(1l).getEdges(BOTH,"created").iterator().hasNext());
        assertTrue(results.get(4l).getEdges(BOTH,"created").iterator().hasNext());
        assertTrue(results.get(6l).getEdges(BOTH,"created").iterator().hasNext());
    }
}
