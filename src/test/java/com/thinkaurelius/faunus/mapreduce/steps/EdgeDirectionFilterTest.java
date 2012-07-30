package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
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
public class EdgeDirectionFilterTest extends BaseTest {
    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new EdgeDirectionFilter.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testMap1() throws IOException {
        Configuration config = new Configuration();
        config.set(EdgeDirectionFilter.ACTION, Tokens.Action.DROP.name());
        config.set(EdgeDirectionFilter.DIRECTION, Direction.BOTH.name());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(BOTH).iterator().hasNext());
            assertFalse(vertex.getEdges(IN).iterator().hasNext());
            assertFalse(vertex.getEdges(OUT).iterator().hasNext());
        }
        assertEquals(12, this.mapReduceDriver.getCounters().findCounter(EdgeDirectionFilter.Counters.EDGES_DROPPED).getValue());
        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(EdgeDirectionFilter.Counters.EDGES_KEPT).getValue());
    }

    public void testMap2() throws IOException {
        Configuration config = new Configuration();
        config.set(EdgeDirectionFilter.ACTION, Tokens.Action.DROP.name());
        config.set(EdgeDirectionFilter.DIRECTION, Direction.OUT.name());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(OUT).iterator().hasNext());
            assertEquals(asList(vertex.getEdges(IN)).size(), asList(vertex.getEdges(BOTH)).size());
        }
        assertEquals(asList(results.get(3l).getEdges(IN)).size(), 3);

        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(EdgeDirectionFilter.Counters.EDGES_DROPPED).getValue());
        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(EdgeDirectionFilter.Counters.EDGES_KEPT).getValue());
    }

    public void testMap3() throws IOException {
        Configuration config = new Configuration();
        config.set(EdgeDirectionFilter.ACTION, Tokens.Action.DROP.name());
        config.set(EdgeDirectionFilter.DIRECTION, Direction.IN.name());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final FaunusVertex vertex : results.values()) {
            assertFalse(vertex.getEdges(IN).iterator().hasNext());
            assertEquals(asList(vertex.getEdges(OUT)).size(), asList(vertex.getEdges(BOTH)).size());
        }
        assertEquals(asList(results.get(1l).getEdges(OUT)).size(), 3);

        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(EdgeDirectionFilter.Counters.EDGES_DROPPED).getValue());
        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(EdgeDirectionFilter.Counters.EDGES_KEPT).getValue());
    }
}
