package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

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

    /*
        // TODO: Assumptions around breadth- vs. depth-first traversal
        public void testVertexSideEffect() throws Exception {
        Configuration config = new Configuration();
        config.setClass(SideEffectMap.CLASS, Vertex.class, Element.class);
        config.set(SideEffectMap.CLOSURE, "{it -> if(it.count) {it.count++} else {it.count=1}}");
        config.setBoolean(FaunusCompiler.PATH_ENABLED, false);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class, 1, 1, 1, 2, 3, 3), mapReduceDriver);

        assertEquals(graph.size(), 6);
        assertEquals(graph.get(1l).getProperty("count"), 3);
        assertEquals(graph.get(2l).getProperty("count"), 1);
        assertEquals(graph.get(3l).getProperty("count"), 2);
        assertNull(graph.get(4l).getProperty("count"));
        assertNull(graph.get(5l).getProperty("count"));
        assertNull(graph.get(6l).getProperty("count"));


        assertEquals(mapReduceDriver.getCounters().findCounter(SideEffectMap.Counters.VERTICES_PROCESSED).getValue(), 3);
        assertEquals(mapReduceDriver.getCounters().findCounter(SideEffectMap.Counters.OUT_EDGES_PROCESSED).getValue(), 0);
    }*/

    public void testVertexSideEffectOutDegree() throws Exception {
        Configuration config = SideEffectMap.createConfiguration(Vertex.class, "{it -> it.degree = it.outE().count()}");
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).getProperty("degree"), 3l);
        assertEquals(results.get(2l).getProperty("degree"), 0l);
        assertEquals(results.get(3l).getProperty("degree"), 0l);
        assertEquals(results.get(4l).getProperty("degree"), 2l);
        assertEquals(results.get(5l).getProperty("degree"), 0l);
        assertEquals(results.get(6l).getProperty("degree"), 1l);

        assertEquals(mapReduceDriver.getCounters().findCounter(SideEffectMap.Counters.VERTICES_PROCESSED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(SideEffectMap.Counters.OUT_EDGES_PROCESSED).getValue(), 0);
    }


    public void testVertexSideEffectInDegree() throws Exception {

        Configuration config = SideEffectMap.createConfiguration(Vertex.class, "{it -> it.degree = it.inE.count()}");
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), Vertex.class), mapReduceDriver);

        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).getProperty("degree"), 0l);
        assertEquals(results.get(2l).getProperty("degree"), 1l);
        assertEquals(results.get(3l).getProperty("degree"), 3l);
        assertEquals(results.get(4l).getProperty("degree"), 1l);
        assertEquals(results.get(5l).getProperty("degree"), 1l);
        assertEquals(results.get(6l).getProperty("degree"), 0l);

        assertEquals(mapReduceDriver.getCounters().findCounter(SideEffectMap.Counters.VERTICES_PROCESSED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(SideEffectMap.Counters.OUT_EDGES_PROCESSED).getValue(), 0);
    }

}
