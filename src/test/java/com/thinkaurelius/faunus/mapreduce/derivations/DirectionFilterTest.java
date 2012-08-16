package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.derivations.DirectionFilter;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DirectionFilterTest extends BaseTest {
    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new DirectionFilter.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testDropBoth() throws IOException {
        Configuration config = new Configuration();
        config.set(DirectionFilter.ACTION, Tokens.Action.DROP.name());
        config.set(DirectionFilter.DIRECTION, Direction.BOTH.name());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(BOTH).iterator().hasNext());
            assertFalse(vertex.getEdges(IN).iterator().hasNext());
            assertFalse(vertex.getEdges(OUT).iterator().hasNext());
        }
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(DirectionFilter.Counters.EDGES_DROPPED).getValue());
        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(DirectionFilter.Counters.EDGES_KEPT).getValue());
    }

    public void testDropOut() throws IOException {
        Configuration config = new Configuration();
        config.set(DirectionFilter.ACTION, Tokens.Action.DROP.name());
        config.set(DirectionFilter.DIRECTION, Direction.OUT.name());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(OUT).iterator().hasNext());
            assertEquals(asList(vertex.getEdges(IN)).size(), asList(vertex.getEdges(BOTH)).size());
        }
        assertEquals(asList(results.get(3l).getEdges(IN)).size(), 3);

        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(DirectionFilter.Counters.EDGES_DROPPED).getValue());
        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(DirectionFilter.Counters.EDGES_KEPT).getValue());
    }

    public void testDropIn() throws IOException {
        Configuration config = new Configuration();
        config.set(DirectionFilter.ACTION, Tokens.Action.DROP.name());
        config.set(DirectionFilter.DIRECTION, Direction.IN.name());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(IN).iterator().hasNext());
            assertEquals(asList(vertex.getEdges(OUT)).size(), asList(vertex.getEdges(BOTH)).size());
        }
        assertEquals(asList(results.get(1l).getEdges(OUT)).size(), 3);

        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(DirectionFilter.Counters.EDGES_DROPPED).getValue());
        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(DirectionFilter.Counters.EDGES_KEPT).getValue());
    }
}
