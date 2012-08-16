package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.mapreduce.derivations.Identity;
import com.thinkaurelius.faunus.mapreduce.derivations.VertexPropertyValueFilter;
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
public class MapReduceSequenceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new MapReduceSequence.Map());
        mapReduceDriver.setReducer(new MapReduceSequence.Reduce());
    }

    public void testVertexFiltering() throws IOException {
        Configuration config = new Configuration();
        config.set(VertexPropertyValueFilter.KEY, "age");
        config.set(VertexPropertyValueFilter.COMPARE , Query.Compare.LESS_THAN.name());
        config.set(VertexPropertyValueFilter.VALUE, "30");
        config.set(VertexPropertyValueFilter.VALUE_CLASS , Float.class.getName());
        config.setStrings(MapReduceSequence.MAP_CLASSES, Identity.Map.class.getName(), Identity.Map.class.getName(), Identity.Map.class.getName());
        config.set(MapReduceSequence.MAPR_CLASS, VertexPropertyValueFilter.Map.class.getName());
        config.set(MapReduceSequence.REDUCE_CLASS, VertexPropertyValueFilter.Reduce.class.getName());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(BaseTest.ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 2);
        assertTrue(results.containsKey(1l));
        assertTrue(results.containsKey(2l));
    }

    public void testMapReduceOneJob() throws IOException {
        Configuration config = new Configuration();
        config.set(VertexPropertyValueFilter.KEY, "age");
        config.set(VertexPropertyValueFilter.COMPARE , Query.Compare.LESS_THAN.name());
        config.set(VertexPropertyValueFilter.VALUE, "30");
        config.set(VertexPropertyValueFilter.VALUE_CLASS , Float.class.getName());
        config.set(MapReduceSequence.MAPR_CLASS, VertexPropertyValueFilter.Map.class.getName());
        config.set(MapReduceSequence.REDUCE_CLASS, VertexPropertyValueFilter.Reduce.class.getName());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(BaseTest.ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 2);
        assertTrue(results.containsKey(1l));
        assertTrue(results.containsKey(2l));
    }
}
