package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.derivations.Identity;
import com.thinkaurelius.faunus.mapreduce.derivations.LabelFilter;
import com.thinkaurelius.faunus.mapreduce.derivations.Traverse;
import com.thinkaurelius.faunus.mapreduce.derivations.VertexPropertyFilter;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.OUT;

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
        config.set(VertexPropertyFilter.KEY, "age");
        config.set(VertexPropertyFilter.COMPARE, Query.Compare.LESS_THAN.name());
        config.set(VertexPropertyFilter.VALUE, "30");
        config.set(VertexPropertyFilter.VALUE_CLASS, Float.class.getName());
        config.setStrings(MapReduceSequence.MAP_CLASSES, Identity.Map.class.getName(), Identity.Map.class.getName(), Identity.Map.class.getName());
        config.set(MapReduceSequence.MAPR_CLASS, VertexPropertyFilter.Map.class.getName());
        config.set(MapReduceSequence.REDUCE_CLASS, VertexPropertyFilter.Reduce.class.getName());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(BaseTest.ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 2);
        assertTrue(results.containsKey(1l));
        assertTrue(results.containsKey(2l));
    }

    public void testMapReduceOneJob() throws IOException {
        Configuration config = new Configuration();
        config.set(VertexPropertyFilter.KEY, "age");
        config.set(VertexPropertyFilter.COMPARE, Query.Compare.LESS_THAN.name());
        config.set(VertexPropertyFilter.VALUE, "30");
        config.set(VertexPropertyFilter.VALUE_CLASS, Float.class.getName());
        config.set(MapReduceSequence.MAPR_CLASS, VertexPropertyFilter.Map.class.getName());
        config.set(MapReduceSequence.REDUCE_CLASS, VertexPropertyFilter.Reduce.class.getName());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(BaseTest.ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 2);
        assertTrue(results.containsKey(1l));
        assertTrue(results.containsKey(2l));
    }

    public void testMapReduceLabelFilterTraverse() throws IOException {
        Configuration config = new Configuration();
        config.set(LabelFilter.ACTION + "-0", Tokens.Action.KEEP.name());
        config.setStrings(LabelFilter.LABELS + "-0", "father");

        config.set(Traverse.FIRST_LABEL, "father");
        config.set(Traverse.SECOND_LABEL, "father");
        config.set(Traverse.NEW_LABEL, "grandfather");
        config.set(Traverse.FIRST_DIRECTION, OUT.toString());
        config.set(Traverse.SECOND_DIRECTION, OUT.toString());
        config.set(Traverse.ACTION, Tokens.Action.DROP.toString());

        config.setStrings(MapReduceSequence.MAP_CLASSES, LabelFilter.Map.class.getName());
        config.set(MapReduceSequence.MAPR_CLASS, Traverse.Map.class.getName());
        config.set(MapReduceSequence.REDUCE_CLASS, Traverse.Reduce.class.getName());

        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.GRAPH_OF_THE_GODS, this.mapReduceDriver);
        for (Vertex vertex : results.values()) {
            System.out.println(getFullString(vertex));
        }
    }
}
