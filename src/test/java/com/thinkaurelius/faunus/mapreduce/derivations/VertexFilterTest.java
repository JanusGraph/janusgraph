package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexFilterTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new VertexFilter.Map());
        mapReduceDriver.setReducer(new VertexFilter.Reduce());
    }

    public void testOldVerticesFiltered() throws IOException {
        Configuration config = new Configuration();
        config.set(VertexFilter.CLOSURE, "{it -> it.age != null && it.age < 35}");

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

        assertEquals(3l, this.mapReduceDriver.getCounters().findCounter(VertexFilter.Counters.VERTICES_DROPPED).getValue());
        assertEquals(3l, this.mapReduceDriver.getCounters().findCounter(VertexFilter.Counters.VERTICES_KEPT).getValue());
    }
}
