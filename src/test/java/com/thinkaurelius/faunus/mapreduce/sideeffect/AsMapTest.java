package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AsMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new AsMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testKnowsCreatedTraversal() throws IOException {
        Configuration config = new Configuration();
        config.setClass(AsMap.CLASS, Vertex.class, Element.class);
        config.set(AsMap.TAG, "a");

        mapReduceDriver.withConfiguration(config);

        /*Map<Long, FaunusVertex> results = generateIndexedToyGraph(BaseTest.ExampleGraph.TINKERGRAPH);
        results.get(1l).setEnergy(1);
        results.get(2l).setEnergy(0);
        results.get(3l).setEnergy(2);
        results.get(4l).setEnergy(0);
        results.get(5l).setEnergy(0);
        results.get(6l).setEnergy(3);

        results = runWithGraph(results.values(), mapReduceDriver);

        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).getTag(), 'a');
        assertEquals(results.get(2l).getTag(), '_');
        assertEquals(results.get(3l).getTag(), 'a');
        assertEquals(results.get(4l).getTag(), '_');
        assertEquals(results.get(5l).getTag(), '_');
        assertEquals(results.get(6l).getTag(), 'a');    */
    }
}
