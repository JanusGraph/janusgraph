package com.thinkaurelius.titan.hadoop.mapreduce;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityMapTest extends BaseTest {

    MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex>();
        mapReduceDriver.setMapper(new IdentityMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, HadoopVertex, NullWritable, HadoopVertex>());
    }

    public void testIdentityNoPaths() throws Exception {
        mapReduceDriver.withConfiguration(new Configuration());

        Map<Long, HadoopVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, new Configuration()), mapReduceDriver);

        assertEquals(graph.size(), 6);
        for (HadoopVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 0);
            assertFalse(vertex.hasPaths());
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((StandardFaunusEdge) edge).pathCount(), 0);
                assertFalse(((StandardFaunusEdge) edge).hasPaths());
            }
        }

        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.VERTEX_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.VERTEX_COUNT).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.OUT_EDGE_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.OUT_EDGE_COUNT).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.IN_EDGE_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.IN_EDGE_COUNT).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.VERTEX_PROPERTY_COUNT), 12);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.VERTEX_PROPERTY_COUNT).getValue(), 12);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.OUT_EDGE_PROPERTY_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.OUT_EDGE_PROPERTY_COUNT).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.IN_EDGE_PROPERTY_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.IN_EDGE_PROPERTY_COUNT).getValue(), 6);

        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }

    public void testIdentityPaths() throws Exception {
        mapReduceDriver.withConfiguration(new Configuration());

        Map<Long, HadoopVertex> graph = runWithGraph(startPath(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, new Configuration()), Vertex.class), mapReduceDriver);

        assertEquals(graph.size(), 6);
        for (HadoopVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 1);
            assertTrue(vertex.hasPaths());
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((StandardFaunusEdge) edge).pathCount(), 0);
                assertFalse(((StandardFaunusEdge) edge).hasPaths());
            }
        }

        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.VERTEX_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.VERTEX_COUNT).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.OUT_EDGE_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.OUT_EDGE_COUNT).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.IN_EDGE_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.IN_EDGE_COUNT).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.VERTEX_PROPERTY_COUNT), 12);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.VERTEX_PROPERTY_COUNT).getValue(), 12);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.OUT_EDGE_PROPERTY_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.OUT_EDGE_PROPERTY_COUNT).getValue(), 6);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, IdentityMap.Counters.IN_EDGE_PROPERTY_COUNT), 6);
//        assertEquals(mapReduceDriver.getCounters().findCounter(IdentityMap.Counters.IN_EDGE_PROPERTY_COUNT).getValue(), 6);

        identicalStructure(graph, BaseTest.ExampleGraph.TINKERGRAPH);
    }
}
