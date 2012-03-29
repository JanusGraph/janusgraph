package com.thinkaurelius.faunus.mapreduce.algebra;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.algebra.util.Counters;
import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LabelFilterTest extends BaseTest {

    MapDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapDriver;

    public void setUp() throws Exception {
        mapDriver = new MapDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapDriver.setMapper(new LabelFilter.Map());
    }

    public void testMap1() throws IOException {
        mapDriver.resetOutput();

        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");
        vertex1.addOutEdge(new FaunusEdge(new FaunusVertex(1), new FaunusVertex(2), "knows"));
        vertex1.addOutEdge(new FaunusEdge(new FaunusVertex(1), new FaunusVertex(3), "created"));

        mapDriver.withInput(NullWritable.get(), vertex1);
        List<Pair<NullWritable, FaunusVertex>> list = mapDriver.run();
        assertEquals(list.size(), 1);

        FaunusVertex vertex2 = list.get(0).getSecond();
        List<Edge> edges = BaseTest.asList(vertex2.getOutEdges());
        assertEquals(edges.size(), 2);
        assertTrue(edges.get(0).getLabel().equals("knows") || edges.get(0).getLabel().equals("created"));
        assertTrue(edges.get(1).getLabel().equals("knows") || edges.get(1).getLabel().equals("created"));

        assertEquals(mapDriver.getCounters().findCounter(Counters.EDGES_ALLOWED_BY_LABEL).getValue(), 2);
        assertEquals(mapDriver.getCounters().findCounter(Counters.EDGES_FILTERED_BY_LABEL).getValue(), 0);
    }

    public void testMap2() throws IOException {
        mapDriver.resetOutput();

        Configuration config = new Configuration();
        config.setStrings(LabelFilter.LABELS, "knows");
        mapDriver.withConfiguration(config);

        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");
        vertex1.addOutEdge(new FaunusEdge(new FaunusVertex(1), new FaunusVertex(2), "knows"));
        vertex1.addOutEdge(new FaunusEdge(new FaunusVertex(1), new FaunusVertex(3), "created"));

        mapDriver.withInput(NullWritable.get(), vertex1);
        List<Pair<NullWritable, FaunusVertex>> list = mapDriver.run();
        assertEquals(list.size(), 1);

        FaunusVertex vertex2 = list.get(0).getSecond();
        List<Edge> edges = BaseTest.asList(vertex2.getOutEdges());
        assertEquals(edges.size(), 1);
        assertEquals(edges.get(0).getLabel(), "knows");

        assertEquals(mapDriver.getCounters().findCounter(Counters.EDGES_ALLOWED_BY_LABEL).getValue(), 1);
        assertEquals(mapDriver.getCounters().findCounter(Counters.EDGES_FILTERED_BY_LABEL).getValue(), 1);
    }

}
