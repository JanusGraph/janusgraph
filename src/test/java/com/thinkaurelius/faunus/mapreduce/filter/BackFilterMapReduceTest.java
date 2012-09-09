package com.thinkaurelius.faunus.mapreduce.filter;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.util.MicroVertex;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackFilterMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new BackFilterMapReduce.Map());
        mapReduceDriver.setReducer(new BackFilterMapReduce.Reduce());
    }

    public void testTrue() {
        assertTrue(true);
    }

    public void testVerticesFullStart() throws IOException {
        Configuration config = new Configuration();
        config.setClass(BackFilterMapReduce.CLASS, Vertex.class, Element.class);
        config.setInt(BackFilterMapReduce.STEP, 0);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH), Vertex.class, true), mapReduceDriver);

        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 1);
        assertEquals(results.get(2l).pathCount(), 1);
        assertEquals(results.get(3l).pathCount(), 1);
        assertEquals(results.get(4l).pathCount(), 1);
        assertEquals(results.get(5l).pathCount(), 1);
        assertEquals(results.get(6l).pathCount(), 1);

        assertEquals(mapReduceDriver.getCounters().findCounter(FilterMap.Counters.VERTICES_FILTERED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(FilterMap.Counters.EDGES_FILTERED).getValue(), 0);

        identicalStructure(results, ExampleGraph.TINKERGRAPH);
    }

    public void testVerticesBiasedStart() throws IOException {
        Configuration config = new Configuration();
        config.setClass(BackFilterMapReduce.CLASS, Vertex.class, Element.class);
        config.setInt(BackFilterMapReduce.STEP, 0);
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);

        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = generateIndexedGraph(BaseTest.ExampleGraph.TINKERGRAPH);

        for(FaunusVertex v : results.values()) {
            v.enablePath(true);
        }

        results.get(1l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(1l)), false);
        results.get(2l).addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(2l)), false);
        results.get(3l).addPath((List) Arrays.asList(new MicroVertex(2l), new MicroVertex(3l)), false);
        results.get(4l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroVertex(4l)), false);
        results.get(5l).addPath((List) Arrays.asList(new MicroVertex(3l), new MicroVertex(5l)), false);



        results = runWithGraph(results.values(), mapReduceDriver);

        assertEquals(results.size(), 6);
        assertEquals(results.get(1l).pathCount(), 2);
        assertEquals(results.get(2l).pathCount(), 1);
        assertEquals(results.get(3l).pathCount(), 2);
        assertEquals(results.get(4l).pathCount(), 0);
        assertEquals(results.get(5l).pathCount(), 0);
        assertEquals(results.get(6l).pathCount(), 0);

        assertEquals(mapReduceDriver.getCounters().findCounter(FilterMap.Counters.VERTICES_FILTERED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(FilterMap.Counters.EDGES_FILTERED).getValue(), 0);

        identicalStructure(results, ExampleGraph.TINKERGRAPH);
    }

}
