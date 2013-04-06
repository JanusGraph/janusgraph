package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TransformMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, Text, NullWritable, Text> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, Text, NullWritable, Text>();
        mapReduceDriver.setMapper(new TransformMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, Text, NullWritable, Text>());
    }

    public void testVerticesPropertyKeySize() throws Exception {
        Configuration config = TransformMap.createConfiguration(Vertex.class, "{it -> it.propertyKeys.size()}");
        mapReduceDriver.withConfiguration(config);

        final List<Pair<NullWritable, Text>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final Pair<NullWritable, Text> result : results) {
            assertEquals(result.getSecond().toString(), "2");
        }
        assertEquals(mapReduceDriver.getCounters().findCounter(TransformMap.Counters.VERTICES_PROCESSED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(TransformMap.Counters.EDGES_PROCESSED).getValue(), 0);
    }

    public void testVerticesPropertyKeySizeWithPaths() throws Exception {
        Configuration config = TransformMap.createConfiguration(Vertex.class, "{it -> it.propertyKeys.size()}");
        config.setBoolean(FaunusCompiler.PATH_ENABLED, true);
        mapReduceDriver.withConfiguration(config);


        final List<Pair<NullWritable, Text>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Vertex.class), this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final Pair<NullWritable, Text> result : results) {
            assertEquals(result.getSecond().toString(), "2");
        }
        assertEquals(mapReduceDriver.getCounters().findCounter(TransformMap.Counters.VERTICES_PROCESSED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(TransformMap.Counters.EDGES_PROCESSED).getValue(), 0);
    }

    public void testEdgesPropertyKeySize() throws Exception {
        Configuration config = TransformMap.createConfiguration(Edge.class, "{it -> it.propertyKeys.size()}");
        mapReduceDriver.withConfiguration(config);

        final List<Pair<NullWritable, Text>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.TINKERGRAPH, config), Edge.class), this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final Pair<NullWritable, Text> result : results) {
            assertEquals(result.getSecond().toString(), "1");
        }

        assertEquals(mapReduceDriver.getCounters().findCounter(TransformMap.Counters.VERTICES_PROCESSED).getValue(), 0);
        assertEquals(mapReduceDriver.getCounters().findCounter(TransformMap.Counters.EDGES_PROCESSED).getValue(), 6);
    }
}
