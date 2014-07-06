package com.thinkaurelius.titan.hadoop.mapreduce.sideeffect;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;
import com.tinkerpop.blueprints.Direction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, HadoopVertex, LongWritable, Holder, NullWritable, HadoopVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, LongWritable, Holder, NullWritable, HadoopVertex>();
        mapReduceDriver.setMapper(new LinkMapReduce.Map());
        mapReduceDriver.setCombiner(new LinkMapReduce.Combiner());
        mapReduceDriver.setReducer(new LinkMapReduce.Reduce());
    }

    public void testKnowsCreatedTraversal() throws Exception {

        Configuration config = LinkMapReduce.createConfiguration(Direction.IN, "knowsCreated", 0, null);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);

        for (HadoopVertex vertex : graph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.BOTH);

        }
        graph.get(3l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(1l), new HadoopVertex.MicroVertex(3l)), false);
        graph.get(5l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(1l), new HadoopVertex.MicroVertex(5l)), false);

        graph = runWithGraph(graph, mapReduceDriver);
        assertEquals(asList(graph.get(1l).getEdges(OUT, "knowsCreated")).size(), 2);
        assertEquals(asList(graph.get(1l).getEdges(BOTH)).size(), 2);
        assertFalse(graph.get(2l).getEdges(BOTH).iterator().hasNext());
        assertEquals(asList(graph.get(3l).getEdges(IN, "knowsCreated")).size(), 1);
        assertEquals(asList(graph.get(3l).getEdges(BOTH)).size(), 1);
        assertFalse(graph.get(4l).getEdges(BOTH).iterator().hasNext());
        assertEquals(asList(graph.get(5l).getEdges(IN, "knowsCreated")).size(), 1);
        assertEquals(asList(graph.get(5l).getEdges(BOTH)).size(), 1);
        assertFalse(graph.get(6l).getEdges(BOTH).iterator().hasNext());

//        assertEquals(mapReduceDriver.getCounters().findCounter(LinkMapReduce.Counters.OUT_EDGES_CREATED).getValue(), 2);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, LinkMapReduce.Counters.OUT_EDGES_CREATED), 2);
//        assertEquals(mapReduceDriver.getCounters().findCounter(LinkMapReduce.Counters.IN_EDGES_CREATED).getValue(), 2);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, LinkMapReduce.Counters.IN_EDGES_CREATED), 2);
    }

    public void testCreated2Traversal() throws Exception {

        Configuration config = LinkMapReduce.createConfiguration(Direction.OUT, "created2", 0, null);
        mapReduceDriver.withConfiguration(config);

        Map<Long, HadoopVertex> graph = generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, config);
        for (HadoopVertex vertex : graph.values()) {
            vertex.removeEdges(Tokens.Action.DROP, Direction.BOTH);
        }
        graph.get(3l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(1l), new HadoopVertex.MicroVertex(3l)), false);
        graph.get(5l).addPath((List) Arrays.asList(new HadoopVertex.MicroVertex(1l), new HadoopVertex.MicroVertex(5l)), false);

        graph = runWithGraph(graph, mapReduceDriver);
        assertEquals(asList(graph.get(1l).getEdges(IN, "created2")).size(), 2);
        assertEquals(asList(graph.get(1l).getEdges(BOTH)).size(), 2);
        assertFalse(graph.get(2l).getEdges(BOTH).iterator().hasNext());
        assertEquals(asList(graph.get(3l).getEdges(OUT, "created2")).size(), 1);
        assertEquals(asList(graph.get(3l).getEdges(BOTH)).size(), 1);
        assertFalse(graph.get(4l).getEdges(BOTH).iterator().hasNext());
        assertEquals(asList(graph.get(5l).getEdges(OUT, "created2")).size(), 1);
        assertEquals(asList(graph.get(5l).getEdges(BOTH)).size(), 1);
        assertFalse(graph.get(6l).getEdges(BOTH).iterator().hasNext());

//        assertEquals(mapReduceDriver.getCounters().findCounter(LinkMapReduce.Counters.OUT_EDGES_CREATED).getValue(), 2);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, LinkMapReduce.Counters.OUT_EDGES_CREATED), 2);
//        assertEquals(mapReduceDriver.getCounters().findCounter(LinkMapReduce.Counters.IN_EDGES_CREATED).getValue(), 2);
        assertEquals(HadoopCompatLoader.getDefaultCompat().getCounter(mapReduceDriver, LinkMapReduce.Counters.IN_EDGES_CREATED), 2);
    }
}

