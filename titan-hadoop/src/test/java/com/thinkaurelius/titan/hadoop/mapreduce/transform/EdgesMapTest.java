package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgesMapTest extends BaseTest {

    MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex>();
        mapReduceDriver.setMapper(new EdgesMap.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, HadoopVertex, NullWritable, HadoopVertex>());
    }

    public void testEdges() throws Exception {
        mapReduceDriver.withConfiguration(new Configuration());

        Map<Long, HadoopVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, new Configuration()), mapReduceDriver);

        assertEquals(graph.size(), 6);
        for (HadoopVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((HadoopEdge) edge).pathCount(), 1);
            }
        }

        assertEquals(mapReduceDriver.getCounters().findCounter(EdgesMap.Counters.IN_EDGES_PROCESSED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgesMap.Counters.OUT_EDGES_PROCESSED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgesMap.Counters.VERTICES_PROCESSED).getValue(), 6);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }

    public void testEdgesWithPaths() throws Exception {
        Configuration config = EdgesMap.createConfiguration(false);
        config.setBoolean(Tokens.FAUNUS_PIPELINE_TRACK_PATHS, true);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config), mapReduceDriver);

        assertEquals(graph.size(), 6);
        for (HadoopVertex vertex : graph.values()) {
            assertEquals(vertex.pathCount(), 0);
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                assertEquals(((HadoopEdge) edge).pathCount(), 1);
                assertEquals(((HadoopEdge) edge).getPaths().get(0).size(), 1);
                assertEquals(((HadoopEdge) edge).getPaths().get(0).get(0).getId(), edge.getId());
            }
        }

        assertEquals(mapReduceDriver.getCounters().findCounter(EdgesMap.Counters.IN_EDGES_PROCESSED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgesMap.Counters.OUT_EDGES_PROCESSED).getValue(), 6);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgesMap.Counters.VERTICES_PROCESSED).getValue(), 0);

        identicalStructure(graph, ExampleGraph.TINKERGRAPH);
    }
}
