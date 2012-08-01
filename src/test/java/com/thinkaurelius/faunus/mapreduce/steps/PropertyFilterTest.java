package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyFilterTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new PropertyFilter.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testDropNameProperty() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(PropertyFilter.KEYS, "name");
        config.set(PropertyFilter.ACTION, Tokens.Action.DROP.name());
        config.set(PropertyFilter.CLASS, Vertex.class.getName());
        this.mapReduceDriver.withConfiguration(config);
        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);

        // vertex 1
        FaunusVertex vertex = results.get(1l);
        assertEquals(vertex.getPropertyKeys().size(), 1);
        assertEquals(vertex.getProperty("age"), 29);
        assertEquals(asList(vertex.getEdges(OUT)).size(), 3);
        assertEquals(asList(vertex.getEdges(IN)).size(), 0);

        for (final FaunusVertex v : results.values()) {
            assertFalse(v.getPropertyKeys().contains("name"));
        }

        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(PropertyFilter.Counters.EDGE_PROPERTIES_DROPPED).getValue());
        assertEquals(0, this.mapReduceDriver.getCounters().findCounter(PropertyFilter.Counters.EDGE_PROPERTIES_KEPT).getValue());
        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(PropertyFilter.Counters.VERTEX_PROPERTIES_DROPPED).getValue());
        assertEquals(6, this.mapReduceDriver.getCounters().findCounter(PropertyFilter.Counters.VERTEX_PROPERTIES_KEPT).getValue());
    }

    // TODO TEST EDGE PROPERTY FILTERING


}
