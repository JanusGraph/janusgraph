package com.thinkaurelius.faunus.mapreduce.derivations;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CloseLineTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new CloseLine.Map());
        mapReduceDriver.setReducer(new Reducer<NullWritable, FaunusVertex, NullWritable, FaunusVertex>());
    }

    public void testMapReduce1() throws IOException {
        FaunusVertex vertex1 = new FaunusVertex(1);
        vertex1.setProperty("name", "marko");
        FaunusVertex vertex2 = new FaunusVertex(2);
        vertex2.setProperty("name", "gremlin");
        vertex1.addEdge(OUT, new FaunusEdge(vertex1.getIdAsLong(), vertex2.getIdAsLong(), "created"));
        vertex2.addEdge(IN, new FaunusEdge(vertex1.getIdAsLong(), vertex2.getIdAsLong(), "created"));

        Configuration config = new Configuration();
        config.setStrings(CloseLine.LABELS, "created","blah","nothing");
        config.set(CloseLine.NEW_LABEL, "createdBy");
        config.set(CloseLine.ACTION, Tokens.Action.KEEP.name());
        config.set(CloseLine.NEW_DIRECTION, Direction.IN.name());
        mapReduceDriver.withConfiguration(config);
        mapReduceDriver.withInput(NullWritable.get(), vertex1).withInput(NullWritable.get(), vertex2);
        List<Pair<NullWritable, FaunusVertex>> list = mapReduceDriver.run();
        assertEquals(list.size(), 2);
        for (Pair<NullWritable, FaunusVertex> pair : list) {
            assertEquals(pair.getFirst(), NullWritable.get());
            FaunusVertex temp = pair.getSecond();
            if (temp.getId().equals(1l)) {
                List<Edge> edges = asList(temp.getEdges(Direction.OUT));
                assertEquals(edges.size(), 1);
                assertEquals(edges.get(0).getLabel(), "created");

                edges = asList(temp.getEdges(Direction.IN));
                assertEquals(edges.size(), 1);
                assertEquals(edges.get(0).getLabel(), "createdBy");

                assertEquals(temp.getPropertyKeys().size(), 1);
                assertEquals(temp.getProperty("name"), "marko");
            } else {
                List<Edge> edges = asList(temp.getEdges(Direction.OUT));
                assertEquals(edges.size(), 1);
                assertEquals(edges.get(0).getLabel(), "createdBy");
                assertEquals(edges.get(0).getVertex(Direction.IN), vertex1);
                assertEquals(edges.get(0).getVertex(Direction.OUT), vertex2);

                edges = asList(temp.getEdges(Direction.IN));
                assertEquals(edges.size(), 1);
                assertEquals(edges.get(0).getLabel(), "created");
                assertEquals(edges.get(0).getVertex(Direction.IN), vertex2);
                assertEquals(edges.get(0).getVertex(Direction.OUT), vertex1);

                assertEquals(temp.getPropertyKeys().size(), 1);
                assertEquals(temp.getProperty("name"), "gremlin");
            }
        }

        //assertEquals(mapReduceDriver.getCounters().findCounter(Counters.EDGES_TRANSPOSED).getValue(), 1);
    }

    public void testMapReduce2() throws IOException {
        Configuration config = new Configuration();
        config.set(CloseLine.ACTION, Tokens.Action.DROP.name());
        config.set(CloseLine.LABELS, "created");
        config.set(CloseLine.NEW_LABEL, "createdBy");
        config.set(CloseLine.NEW_DIRECTION, Direction.IN.name());
        mapReduceDriver.withConfiguration(config);

        Map<Long, FaunusVertex> results = runWithToyGraph(ExampleGraph.TINKERGRAPH, this.mapReduceDriver);
        assertEquals(results.size(), 6);

        FaunusVertex marko = results.get(1l);
        assertEquals(marko.getProperty("name"), "marko");
        assertEquals(asList(marko.getEdges(OUT)).size(), 2);
        assertTrue(getVertices(marko.getEdges(OUT), IN).contains(results.get(2l)));
        assertTrue(getVertices(marko.getEdges(OUT), IN).contains(results.get(4l)));
        assertEquals(asList(marko.getEdges(IN)).size(), 1);

        FaunusVertex ripple = results.get(5l);
        assertEquals(ripple.getProperty("name"), "ripple");
        assertEquals(asList(ripple.getEdges(OUT)).size(), 1);
        assertEquals(asList(ripple.getEdges(IN)).size(), 0);
        assertEquals(ripple.getEdges(OUT).iterator().next().getProperty("weight"), 1);
        assertEquals(ripple.getEdges(OUT).iterator().next().getLabel(), "createdBy");

        FaunusVertex lop = results.get(3l);
        assertEquals(lop.getProperty("name"), "lop");
        assertEquals(asList(lop.getEdges(OUT)).size(), 3);
        assertEquals(asList(lop.getEdges(OUT, "createdBy")).size(), 3);
        assertEquals(asList(lop.getEdges(IN)).size(), 0);

        assertTrue(getVertices(lop.getEdges(OUT), IN).contains(results.get(1l)));
        assertTrue(getVertices(lop.getEdges(OUT), IN).contains(results.get(4l)));
        assertTrue(getVertices(lop.getEdges(OUT), IN).contains(results.get(6l)));

        assertEquals(mapReduceDriver.getCounters().findCounter(CloseLine.Counters.EDGES_TRANSPOSED).getValue(), 8);
    }

}
