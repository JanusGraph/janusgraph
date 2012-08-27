package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TransformMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, FaunusVertex, Text, FaunusVertex, Text> mapReduceDriver;

    public void setUp() throws Exception {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, FaunusVertex, Text, FaunusVertex, Text>();
        mapReduceDriver.setMapper(new TransformMap.Map());
        mapReduceDriver.setReducer(new Reducer<FaunusVertex, Text, FaunusVertex, Text>());
    }

    public void testTransformToPropertyKeySize() throws IOException {
        Configuration config = new Configuration();
        config.set(TransformMap.FUNCTION, "{it -> it.propertyKeys.size()}");
        mapReduceDriver.withConfiguration(config);

        final List<Pair<FaunusVertex, Text>> results = runWithGraphNoIndex(startPath(generateGraph(ExampleGraph.TINKERGRAPH),Vertex.class), this.mapReduceDriver);
        assertEquals(results.size(), 6);
        for (final Pair<FaunusVertex, Text> result : results) {
            assertEquals(result.getSecond().toString(), "2");
        }
        assertEquals(mapReduceDriver.getCounters().findCounter(TransformMap.Counters.VERTICES_PROCESSED).getValue(), 6);
    }
}
