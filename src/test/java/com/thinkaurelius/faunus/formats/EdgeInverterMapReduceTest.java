package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeInverterMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new EdgeInverterMapReduce.Map());
        mapReduceDriver.setCombiner(new EdgeInverterMapReduce.Combiner());
        mapReduceDriver.setReducer(new EdgeInverterMapReduce.Reduce());
    }

    public void testInversionWithTinkerGraphOutEdges() throws Exception {
        mapReduceDriver.setConfiguration(EdgeInverterMapReduce.createConfiguration(Direction.OUT));
        Map<Long, FaunusVertex> halfGraph = BaseTest.generateGraph(ExampleGraph.TINKERGRAPH);
        for (FaunusVertex vertex : halfGraph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.IN);
        }
        Map<Long, FaunusVertex> graph = runWithGraph(startPath(halfGraph, Vertex.class), mapReduceDriver);
        BaseTest.identicalStructure(graph, ExampleGraph.TINKERGRAPH);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgeInverterMapReduce.Counters.EDGES_INVERTED).getValue(),
                mapReduceDriver.getCounters().findCounter(EdgeInverterMapReduce.Counters.EDGES_AGGREGATED).getValue());
    }

    public void testInversionWithTinkerGraphInEdges() throws Exception {
        mapReduceDriver.setConfiguration(EdgeInverterMapReduce.createConfiguration(Direction.IN));
        Map<Long, FaunusVertex> halfGraph = BaseTest.generateGraph(ExampleGraph.TINKERGRAPH);
        for (FaunusVertex vertex : halfGraph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.OUT);
        }
        Map<Long, FaunusVertex> graph = runWithGraph(startPath(halfGraph, Vertex.class), mapReduceDriver);
        BaseTest.identicalStructure(graph, ExampleGraph.TINKERGRAPH);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgeInverterMapReduce.Counters.EDGES_INVERTED).getValue(),
                mapReduceDriver.getCounters().findCounter(EdgeInverterMapReduce.Counters.EDGES_AGGREGATED).getValue());
    }

    public void testInversionWithGraphOfTheGodsOutEdges() throws Exception {
        mapReduceDriver.setConfiguration(EdgeInverterMapReduce.createConfiguration(Direction.OUT));
        Map<Long, FaunusVertex> halfGraph = BaseTest.generateGraph(ExampleGraph.GRAPH_OF_THE_GODS);
        for (FaunusVertex vertex : halfGraph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.IN);
        }
        Map<Long, FaunusVertex> graph = runWithGraph(startPath(halfGraph, Vertex.class), mapReduceDriver);
        BaseTest.identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgeInverterMapReduce.Counters.EDGES_INVERTED).getValue(),
                mapReduceDriver.getCounters().findCounter(EdgeInverterMapReduce.Counters.EDGES_AGGREGATED).getValue());
    }

    public void testInversionWithGraphOfTheGodsInEdges() throws Exception {
        mapReduceDriver.setConfiguration(EdgeInverterMapReduce.createConfiguration(Direction.IN));
        Map<Long, FaunusVertex> halfGraph = BaseTest.generateGraph(ExampleGraph.GRAPH_OF_THE_GODS);
        for (FaunusVertex vertex : halfGraph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.OUT);
        }
        Map<Long, FaunusVertex> graph = runWithGraph(startPath(halfGraph, Vertex.class), mapReduceDriver);
        BaseTest.identicalStructure(graph, ExampleGraph.GRAPH_OF_THE_GODS);
        assertEquals(mapReduceDriver.getCounters().findCounter(EdgeInverterMapReduce.Counters.EDGES_INVERTED).getValue(),
                mapReduceDriver.getCounters().findCounter(EdgeInverterMapReduce.Counters.EDGES_AGGREGATED).getValue());
    }

}
