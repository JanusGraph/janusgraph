package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexValueFilterTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new VertexValueFilter.Map());
        mapReduceDriver.setReducer(new VertexValueFilter.Reduce());
    }

    public void testOldVerticesFiltered() throws IOException {
        Configuration config = new Configuration();
        config.set(VertexValueFilter.KEY, "age");
        config.setClass(VertexValueFilter.VALUE_CLASS, Integer.class, Integer.class);
        config.setStrings(VertexValueFilter.VALUES, "35");
        config.set(VertexValueFilter.COMPARE, Query.Compare.LESS_THAN.name());

        this.mapReduceDriver.withConfiguration(config);
        Map<Long, FaunusVertex> results = runWithToyGraph(BaseTest.ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 3);
        assertNotNull(results.get(1l));
        assertNotNull(results.get(2l));
        assertNotNull(results.get(4l));
        assertNull(results.get(6l));

        for (final FaunusVertex vertex : results.values()) {
            for (final Edge edge : vertex.getEdges(Direction.OUT)) {
                assertEquals(edge.getVertex(Direction.OUT).getId(), vertex.getId());
                Long id = (Long) edge.getVertex(Direction.IN).getId();
                assertTrue(id.equals(1l) || id.equals(4l) || id.equals(2l));
            }

            for (final Edge edge : vertex.getEdges(Direction.IN)) {
                assertEquals(edge.getVertex(Direction.IN).getId(), vertex.getId());
                Long id = (Long) edge.getVertex(Direction.OUT).getId();
                assertTrue(id.equals(1l) || id.equals(4l) || id.equals(2l));
            }
        }

        assertEquals(3l, this.mapReduceDriver.getCounters().findCounter(VertexValueFilter.Counters.VERTICES_DROPPED).getValue());
        assertEquals(3l, this.mapReduceDriver.getCounters().findCounter(VertexValueFilter.Counters.VERTICES_KEPT).getValue());
    }

    public void testOldVerticesFilteredWildCardNull() throws IOException {
        Configuration config = new Configuration();
        config.set(VertexValueFilter.KEY, "age");
        config.set(VertexValueFilter.VALUE_CLASS, Integer.class.getName());
        config.setStrings(VertexValueFilter.VALUES, "35");
        config.set(VertexValueFilter.COMPARE, Query.Compare.LESS_THAN.name());
        config.set(VertexValueFilter.NULL_WILDCARD, "true");

        this.mapReduceDriver.withConfiguration(config);
        Map<Long, FaunusVertex> results = runWithToyGraph(BaseTest.ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 5);
        assertNotNull(results.get(1l));
        assertNotNull(results.get(2l));
        assertNotNull(results.get(3l));
        assertNotNull(results.get(4l));
        assertNotNull(results.get(5l));
        assertNull(results.get(6l));

        for (final FaunusVertex vertex : results.values()) {
            for (final Edge edge : vertex.getEdges(Direction.OUT)) {
                assertEquals(edge.getVertex(Direction.OUT).getId(), vertex.getId());
                Long id = (Long) edge.getVertex(Direction.IN).getId();
                assertTrue(id.equals(1l) || id.equals(2l) || id.equals(3l) || id.equals(4l) || id.equals(5l));
            }

            for (final Edge edge : vertex.getEdges(Direction.IN)) {
                assertEquals(edge.getVertex(Direction.IN).getId(), vertex.getId());
                Long id = (Long) edge.getVertex(Direction.OUT).getId();
                assertTrue(id.equals(1l) || id.equals(2l) || id.equals(3l) || id.equals(4l) || id.equals(5l));
            }
        }

        assertEquals(1l, this.mapReduceDriver.getCounters().findCounter(VertexValueFilter.Counters.VERTICES_DROPPED).getValue());
        assertEquals(5l, this.mapReduceDriver.getCounters().findCounter(VertexValueFilter.Counters.VERTICES_KEPT).getValue());
    }
}