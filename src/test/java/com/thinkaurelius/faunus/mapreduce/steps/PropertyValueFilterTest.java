package com.thinkaurelius.faunus.mapreduce.steps;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
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
public class PropertyValueFilterTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new PropertyValueFilter.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testOldVerticesFiltered() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(PropertyValueFilter.KEY, "age");
        config.set(PropertyValueFilter.CLASS, Vertex.class.getName());
        config.setClass(PropertyValueFilter.VALUE_CLASS, Integer.class, Integer.class);
        config.set(PropertyValueFilter.VALUE, "35");
        config.set(PropertyValueFilter.COMPARE, Query.Compare.LESS_THAN.name());

        this.mapReduceDriver.withConfiguration(config);
        Map<Long, FaunusVertex> results = runWithToyGraph(BaseTest.ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 3);

        assertNotNull(results.get(1l));
        assertNotNull(results.get(4l));
        assertNotNull(results.get(2l));
        assertNull(results.get(6l));

        assertEquals(3l, this.mapReduceDriver.getCounters().findCounter(PropertyValueFilter.Counters.VERTICES_DROPPED).getValue());
        assertEquals(3l, this.mapReduceDriver.getCounters().findCounter(PropertyValueFilter.Counters.VERTICES_KEPT).getValue());
    }

    /*public void testLowWeightedEdgesFiltered() throws IOException {
        Configuration config = new Configuration();
        config.setStrings(PropertyValueFilter.KEY, "weight");
        config.set(PropertyValueFilter.CLASS, Edge.class.getName());
        config.setClass(PropertyValueFilter.VALUE_CLASS, Double.class, Double.class);
        config.setFloat(PropertyValueFilter.VALUE, 0.5f);
        config.set(PropertyValueFilter.COMPARE, Query.Compare.LESS_THAN_EQUAL.name());

        this.mapReduceDriver.withConfiguration(config);
        Map<Long, FaunusVertex> results = runWithToyGraph(BaseTest.ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);
        int numberOfEdges = 0;
        for(final FaunusVertex vertex : results.values()) {
            for(final Edge edge : vertex.getEdges(Direction.BOTH))  {
                assertTrue(((Number)edge.getProperty("weight")).doubleValue() <= 0.5d);
                numberOfEdges++;
            }
        }
        
        assertEquals(numberOfEdges, 8l);
        assertEquals(4l, this.mapReduceDriver.getCounters().findCounter(PropertyValueFilter.Counters.EDGES_DROPPED).getValue());
        assertEquals(8l, this.mapReduceDriver.getCounters().findCounter(PropertyValueFilter.Counters.EDGES_KEPT).getValue());
    }*/
}
