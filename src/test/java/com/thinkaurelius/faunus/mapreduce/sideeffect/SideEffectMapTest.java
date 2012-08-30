package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.util.MicroVertex;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new SideEffectMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testVertexSideEffect() throws IOException {
        Configuration config = new Configuration();
        config.setClass(SideEffectMap.CLASS, Vertex.class, Element.class);
        config.set(SideEffectMap.CLOSURE, "{it -> if(it.count) {it.count++} else {it.count=1}}");

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedGraph(BaseTest.ExampleGraph.TINKERGRAPH);

        results.get(1l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(2l)), false);
        results.get(1l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(3l)), false);
        results.get(1l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(4l)), false);
        results.get(2l).addPath((List) Arrays.asList(new MicroVertex(2l), new MicroVertex(1l)), false);
        results.get(3l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroVertex(4l)), false);
        results.get(3l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroVertex(5l)), false);

        runWithGraph(results.values(), mapReduceDriver);

        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).getProperty("count"), 3);
        assertEquals(results.get(2l).getProperty("count"), 1);
        assertEquals(results.get(3l).getProperty("count"), 2);
        assertNull(results.get(4l).getProperty("count"));
        assertNull(results.get(5l).getProperty("count"));
        assertNull(results.get(6l).getProperty("count"));


        assertEquals(mapReduceDriver.getCounters().findCounter(SideEffectMap.Counters.VERTICES_PROCESSED).getValue(), 3);
        assertEquals(mapReduceDriver.getCounters().findCounter(SideEffectMap.Counters.EDGES_PROCESSED).getValue(), 0);
    }


}
