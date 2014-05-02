package com.thinkaurelius.titan.hadoop.formats;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeCopyMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>, NullWritable, HadoopVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>, NullWritable, HadoopVertex>();
        mapReduceDriver.setMapper(new EdgeCopyMapReduce.Map());
        mapReduceDriver.setReducer(new EdgeCopyMapReduce.Reduce());
    }

    public void testInversionWithTinkerGraphOutEdges() throws Exception {
        mapReduceDriver.setConfiguration(EdgeCopyMapReduce.createConfiguration(Direction.OUT));
        Map<Long, HadoopVertex> halfGraph = BaseTest.generateGraph(ExampleGraph.TINKERGRAPH);
        for (HadoopVertex vertex : halfGraph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.IN);
        }
        Map<Long, HadoopVertex> graph = runWithGraph(startPath(halfGraph, Vertex.class), mapReduceDriver);
        BaseTest.identicalStructure(graph, ExampleGraph.TINKERGRAPH);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgeCopyMapReduce.Counters.EDGES_COPIED).getValue(),
                mapReduceDriver.getCounters().findCounter(EdgeCopyMapReduce.Counters.EDGES_ADDED).getValue());
    }

    public void testInversionWithTinkerGraphInEdges() throws Exception {
        mapReduceDriver.setConfiguration(EdgeCopyMapReduce.createConfiguration(Direction.IN));
        Map<Long, HadoopVertex> halfGraph = BaseTest.generateGraph(ExampleGraph.TINKERGRAPH);
        for (HadoopVertex vertex : halfGraph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.OUT);
        }
        Map<Long, HadoopVertex> graph = runWithGraph(startPath(halfGraph, Vertex.class), mapReduceDriver);
        BaseTest.identicalStructure(graph, ExampleGraph.TINKERGRAPH);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgeCopyMapReduce.Counters.EDGES_COPIED).getValue(),
                mapReduceDriver.getCounters().findCounter(EdgeCopyMapReduce.Counters.EDGES_ADDED).getValue());
    }

    public void testInversionWithGraphOfTheGodsOutEdges() throws Exception {
        mapReduceDriver.setConfiguration(EdgeCopyMapReduce.createConfiguration(Direction.OUT));
        Map<Long, HadoopVertex> halfGraph = BaseTest.generateGraph(ExampleGraph.GRAPH_OF_THE_GODS);
        for (HadoopVertex vertex : halfGraph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.IN);
        }
        Map<Long, HadoopVertex> graph = runWithGraph(startPath(halfGraph, Vertex.class), mapReduceDriver);
        BaseTest.identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgeCopyMapReduce.Counters.EDGES_COPIED).getValue(),
                mapReduceDriver.getCounters().findCounter(EdgeCopyMapReduce.Counters.EDGES_ADDED).getValue());
    }

    public void testInversionWithGraphOfTheGodsInEdges() throws Exception {
        mapReduceDriver.setConfiguration(EdgeCopyMapReduce.createConfiguration(Direction.IN));
        Map<Long, HadoopVertex> halfGraph = BaseTest.generateGraph(ExampleGraph.GRAPH_OF_THE_GODS);
        for (HadoopVertex vertex : halfGraph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.OUT);
        }
        Map<Long, HadoopVertex> graph = runWithGraph(startPath(halfGraph, Vertex.class), mapReduceDriver);
        BaseTest.identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgeCopyMapReduce.Counters.EDGES_COPIED).getValue(),
                mapReduceDriver.getCounters().findCounter(EdgeCopyMapReduce.Counters.EDGES_ADDED).getValue());
    }

}
