package com.thinkaurelius.faunus.mapreduce;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapSequenceTest extends BaseTest {

    MapReduceDriver<Writable, Writable, Writable, Writable, Writable, Writable> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<Writable, Writable, Writable, Writable, Writable, Writable>();
        mapReduceDriver.setMapper(new MapSequence.Map());
        mapReduceDriver.setReducer(new Reducer<Writable, Writable, Writable, Writable>());
    }

    public void testVertexFiltering() throws Exception {
        Configuration config = new Configuration();
        config.setStrings(MapSequence.MAP_CLASSES, IdentityMap.Map.class.getName(), IdentityMap.Map.class.getName(), IdentityMap.Map.class.getName());
        this.mapReduceDriver.withConfiguration(config);
        final Map<Long, FaunusVertex> results = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), mapReduceDriver);
        assertEquals(results.size(), 6);
        identicalStructure(results, ExampleGraph.TINKERGRAPH);
    }

    /*public void testMapReduceOneJob() throws Exception {
        Configuration config = new Configuration();
        config.setStrings(MapSequence.MAP_CLASSES, VerticesVerticesMapReduce.Map.class.getName());
        this.mapReduceDriver.withConfiguration(config);
        this.mapReduceDriver.setReducer((Reducer) new VerticesVerticesMapReduce.Reduce());
        final Map<Long, FaunusVertex> results = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), mapReduceDriver);
        assertEquals(results.size(), 6);
        identicalStructure(results, ExampleGraph.TINKERGRAPH);
    }*/
}
