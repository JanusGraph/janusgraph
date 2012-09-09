package com.thinkaurelius.faunus.mapreduce.transform;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgesMapTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new EdgesMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testEdges() throws IOException {
        mapReduceDriver.withConfiguration(new Configuration());

        Map<Long, FaunusVertex> results = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH), mapReduceDriver);

        assertEquals(results.size(), 6);
        for (FaunusVertex vertex : results.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((FaunusEdge) edge).pathCount(), 1);
//                assertEquals(((FaunusEdge) edge).getPaths().get(0).size(), 1);
//                assertEquals(((FaunusEdge) edge).getPaths().get(0).get(0).getId(), edge.getId());
            }
        }

        assertEquals(mapReduceDriver.getCounters().findCounter(EdgesMap.Counters.EDGES_PROCESSED).getValue(), 12);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgesMap.Counters.VERTICES_PROCESSED).getValue(), 6);

        identicalStructure(results, ExampleGraph.TINKERGRAPH);
    }
}
