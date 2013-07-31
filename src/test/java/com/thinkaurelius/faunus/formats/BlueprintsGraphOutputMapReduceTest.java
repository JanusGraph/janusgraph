package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BlueprintsGraphOutputMapReduceTest extends BaseTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> mapReduceDriver;

    public void setUp() {
        mapReduceDriver = new MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>();
        mapReduceDriver.setMapper(new TinkerGraphOutputMapReduce.Map());
        mapReduceDriver.setReducer(new TinkerGraphOutputMapReduce.Reduce());
    }

    public void testTinkerGraphIncrementalLoading() throws Exception {
        TinkerGraphOutputMapReduce.graph = new TinkerGraph();
        Configuration conf = new Configuration();
        conf.set(BlueprintsGraphOutputMapReduce.FAUNUS_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, "./data/BlueprintsScript.groovy");
        mapReduceDriver.withConfiguration(conf);

        runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, conf), mapReduceDriver);

        Map<Long, FaunusVertex> incrementalGraph = new HashMap<Long, FaunusVertex>();
        // VERTICES
        FaunusVertex marko1 = new FaunusVertex(11l);
        marko1.setProperty("name", "marko");
        marko1.setProperty("height", "5'11");
        FaunusVertex stephen1 = new FaunusVertex(22l);
        stephen1.setProperty("name", "stephen");
        FaunusVertex vadas1 = new FaunusVertex(33l);
        vadas1.setProperty("name", "vadas");
        // EDGES
        marko1.addEdge(Direction.OUT, "worksWith", stephen1.getIdAsLong());
        stephen1.addEdge(Direction.IN, "worksWith", marko1.getIdAsLong());
        marko1.addEdge(Direction.OUT, "worksWith", vadas1.getIdAsLong());
        vadas1.addEdge(Direction.IN, "worksWith", marko1.getIdAsLong());
        stephen1.addEdge(Direction.OUT, "worksWith", vadas1.getIdAsLong());
        vadas1.addEdge(Direction.IN, "worksWith", stephen1.getIdAsLong());
        incrementalGraph.put(11l, marko1);
        incrementalGraph.put(22l, stephen1);
        incrementalGraph.put(33l, vadas1);
        conf = new Configuration();
        conf.set(BlueprintsGraphOutputMapReduce.FAUNUS_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, "./data/BlueprintsScript.groovy");


        setUp();
        mapReduceDriver.withConfiguration(conf);
        Map<Long, FaunusVertex> graph = runWithGraph(incrementalGraph, mapReduceDriver);
        final Graph tinkerGraph = ((TinkerGraphOutputMapReduce.Map) mapReduceDriver.getMapper()).graph;

        Vertex marko = null;
        Vertex peter = null;
        Vertex josh = null;
        Vertex vadas = null;
        Vertex lop = null;
        Vertex ripple = null;
        Vertex stephen = null;
        int count = 0;
        for (Vertex v : tinkerGraph.getVertices()) {
            count++;
            String name = v.getProperty("name").toString();
            if (name.equals("marko")) {
                marko = v;
            } else if (name.equals("peter")) {
                peter = v;
            } else if (name.equals("josh")) {
                josh = v;
            } else if (name.equals("vadas")) {
                vadas = v;
            } else if (name.equals("lop")) {
                lop = v;
            } else if (name.equals("ripple")) {
                ripple = v;
            } else if (name.equals("stephen")) {
                stephen = v;
            } else {
                assertTrue(false);
            }
        }
        assertTrue(null != marko);
        assertTrue(null != peter);
        assertTrue(null != josh);
        assertTrue(null != vadas);
        assertTrue(null != lop);
        assertTrue(null != ripple);
        assertTrue(null != stephen);
        assertEquals(count, 7);

        Set<Vertex> vertices = new HashSet<Vertex>();

        // test marko
        count = 0;
        for (Vertex v : marko.getVertices(Direction.OUT, "worksWith")) {
            count++;
            assertTrue(v.getProperty("name").equals("stephen") || v.getProperty("name").equals("vadas"));
        }
        assertEquals(count, 2);
        assertEquals(marko.getProperty("name"), "marko");
        assertEquals(((Number) marko.getProperty("age")).intValue(), 29);
        assertEquals(marko.getProperty("height"), "5'11");
        assertEquals(marko.getPropertyKeys().size(), 3);

        // test stephen
        count = 0;
        for (Vertex v : stephen.getVertices(Direction.OUT, "worksWith")) {
            count++;
            assertEquals(v.getProperty("name"), "vadas");
        }
        assertEquals(count, 1);
        count = 0;
        for (Vertex v : stephen.getVertices(Direction.IN, "worksWith")) {
            count++;
            assertEquals(v.getProperty("name"), "marko");
        }
        assertEquals(count, 1);
        assertEquals(stephen.getProperty("name"), "stephen");
        assertEquals(stephen.getPropertyKeys().size(), 1);

        // test peter
        vertices = new HashSet<Vertex>();
        assertEquals(peter.getProperty("name"), "peter");
        assertEquals(((Number) peter.getProperty("age")).intValue(), 35);
        assertEquals(peter.getPropertyKeys().size(), 2);
        assertEquals(count(peter.getEdges(Direction.OUT)), 1);
        assertEquals(count(peter.getEdges(Direction.IN)), 0);
        for (Edge e : peter.getEdges(Direction.OUT)) {
            vertices.add(e.getVertex(Direction.IN));
            assertEquals(e.getPropertyKeys().size(), 1);
            assertNotNull(e.getProperty("weight"));
            assertEquals(e.getProperty("weight"), 0.2);
        }
        assertEquals(vertices.size(), 1);
        assertTrue(vertices.contains(lop));
        // test ripple
        vertices = new HashSet<Vertex>();
        assertEquals(ripple.getProperty("name"), "ripple");
        assertEquals(ripple.getProperty("lang"), "java");
        assertEquals(ripple.getPropertyKeys().size(), 2);
        assertEquals(count(ripple.getEdges(Direction.OUT)), 0);
        assertEquals(count(ripple.getEdges(Direction.IN)), 1);
        for (Edge e : ripple.getEdges(Direction.IN)) {
            vertices.add(e.getVertex(Direction.OUT));
            assertEquals(e.getPropertyKeys().size(), 1);
            assertNotNull(e.getProperty("weight"));
            assertEquals(e.getProperty("weight"), 1);
        }
        assertEquals(vertices.size(), 1);
        assertTrue(vertices.contains(josh));
    }

    public void testTinkerGraphMapping() throws Exception {
        TinkerGraphOutputMapReduce.graph = new TinkerGraph();
        mapReduceDriver.withConfiguration(new Configuration());
        final Map<Long, FaunusVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, new Configuration()), mapReduceDriver);
        for (FaunusVertex vertex : graph.values()) {
            assertEquals(count(vertex.getEdges(Direction.IN)), 0);
            assertEquals(count(vertex.getEdges(Direction.OUT)), 0);
            assertEquals(vertex.getProperties().size(), 0);
            assertEquals(vertex.getIdAsLong(), -1);
        }

        final Graph tinkerGraph = ((TinkerGraphOutputMapReduce.Map) mapReduceDriver.getMapper()).graph;

        Vertex marko = null;
        Vertex peter = null;
        Vertex josh = null;
        Vertex vadas = null;
        Vertex lop = null;
        Vertex ripple = null;
        int count = 0;
        for (Vertex v : tinkerGraph.getVertices()) {
            count++;
            String name = v.getProperty("name").toString();
            if (name.equals("marko")) {
                marko = v;
            } else if (name.equals("peter")) {
                peter = v;
            } else if (name.equals("josh")) {
                josh = v;
            } else if (name.equals("vadas")) {
                vadas = v;
            } else if (name.equals("lop")) {
                lop = v;
            } else if (name.equals("ripple")) {
                ripple = v;
            } else {
                assertTrue(false);
            }
        }
        assertEquals(count, 6);
        assertTrue(null != marko);
        assertTrue(null != peter);
        assertTrue(null != josh);
        assertTrue(null != vadas);
        assertTrue(null != lop);
        assertTrue(null != ripple);

        assertEquals(count(tinkerGraph.getEdges()), 6);

        // test marko
        Set<Vertex> vertices = new HashSet<Vertex>();
        assertEquals(marko.getProperty("name"), "marko");
        assertEquals(((Number) marko.getProperty("age")).intValue(), 29);
        assertEquals(marko.getPropertyKeys().size(), 2);
        assertEquals(count(marko.getEdges(Direction.OUT)), 3);
        assertEquals(count(marko.getEdges(Direction.IN)), 0);
        for (Edge e : marko.getEdges(Direction.OUT)) {
            vertices.add(e.getVertex(Direction.IN));
            assertEquals(e.getPropertyKeys().size(), 1);
            assertNotNull(e.getProperty("weight"));
        }
        assertEquals(vertices.size(), 3);
        assertTrue(vertices.contains(lop));
        assertTrue(vertices.contains(josh));
        assertTrue(vertices.contains(vadas));
        // test peter
        vertices = new HashSet<Vertex>();
        assertEquals(peter.getProperty("name"), "peter");
        assertEquals(((Number) peter.getProperty("age")).intValue(), 35);
        assertEquals(peter.getPropertyKeys().size(), 2);
        assertEquals(count(peter.getEdges(Direction.OUT)), 1);
        assertEquals(count(peter.getEdges(Direction.IN)), 0);
        for (Edge e : peter.getEdges(Direction.OUT)) {
            vertices.add(e.getVertex(Direction.IN));
            assertEquals(e.getPropertyKeys().size(), 1);
            assertNotNull(e.getProperty("weight"));
            assertEquals(e.getProperty("weight"), 0.2);
        }
        assertEquals(vertices.size(), 1);
        assertTrue(vertices.contains(lop));
        // test josh
        vertices = new HashSet<Vertex>();
        assertEquals(josh.getProperty("name"), "josh");
        assertEquals(((Number) josh.getProperty("age")).intValue(), 32);
        assertEquals(josh.getPropertyKeys().size(), 2);
        assertEquals(count(josh.getEdges(Direction.OUT)), 2);
        assertEquals(count(josh.getEdges(Direction.IN)), 1);
        for (Edge e : josh.getEdges(Direction.OUT)) {
            vertices.add(e.getVertex(Direction.IN));
            assertEquals(e.getPropertyKeys().size(), 1);
            assertNotNull(e.getProperty("weight"));
        }
        assertEquals(vertices.size(), 2);
        assertTrue(vertices.contains(lop));
        assertTrue(vertices.contains(ripple));
        vertices = new HashSet<Vertex>();
        for (Edge e : josh.getEdges(Direction.IN)) {
            vertices.add(e.getVertex(Direction.OUT));
            assertEquals(e.getPropertyKeys().size(), 1);
            assertNotNull(e.getProperty("weight"));
            assertEquals(e.getProperty("weight"), 1);
        }
        assertEquals(vertices.size(), 1);
        assertTrue(vertices.contains(marko));
        // test vadas
        vertices = new HashSet<Vertex>();
        assertEquals(vadas.getProperty("name"), "vadas");
        assertEquals(((Number) vadas.getProperty("age")).intValue(), 27);
        assertEquals(vadas.getPropertyKeys().size(), 2);
        assertEquals(count(vadas.getEdges(Direction.OUT)), 0);
        assertEquals(count(vadas.getEdges(Direction.IN)), 1);
        for (Edge e : vadas.getEdges(Direction.IN)) {
            vertices.add(e.getVertex(Direction.OUT));
            assertEquals(e.getPropertyKeys().size(), 1);
            assertNotNull(e.getProperty("weight"));
            assertEquals(e.getProperty("weight"), 0.5);
        }
        assertEquals(vertices.size(), 1);
        assertTrue(vertices.contains(marko));
        // test lop
        vertices = new HashSet<Vertex>();
        assertEquals(lop.getProperty("name"), "lop");
        assertEquals(lop.getProperty("lang"), "java");
        assertEquals(lop.getPropertyKeys().size(), 2);
        assertEquals(count(lop.getEdges(Direction.OUT)), 0);
        assertEquals(count(lop.getEdges(Direction.IN)), 3);
        for (Edge e : lop.getEdges(Direction.IN)) {
            vertices.add(e.getVertex(Direction.OUT));
            assertEquals(e.getPropertyKeys().size(), 1);
            assertNotNull(e.getProperty("weight"));
        }
        assertEquals(vertices.size(), 3);
        assertTrue(vertices.contains(marko));
        assertTrue(vertices.contains(josh));
        assertTrue(vertices.contains(peter));
        // test ripple
        vertices = new HashSet<Vertex>();
        assertEquals(ripple.getProperty("name"), "ripple");
        assertEquals(ripple.getProperty("lang"), "java");
        assertEquals(ripple.getPropertyKeys().size(), 2);
        assertEquals(count(ripple.getEdges(Direction.OUT)), 0);
        assertEquals(count(ripple.getEdges(Direction.IN)), 1);
        for (Edge e : ripple.getEdges(Direction.IN)) {
            vertices.add(e.getVertex(Direction.OUT));
            assertEquals(e.getPropertyKeys().size(), 1);
            assertNotNull(e.getProperty("weight"));
            assertEquals(e.getProperty("weight"), 1);
        }
        assertEquals(vertices.size(), 1);
        assertTrue(vertices.contains(josh));
    }

    public static class TinkerGraphOutputMapReduce extends BlueprintsGraphOutputMapReduce {

        private static Graph graph = new TinkerGraph();

        public static Graph getGraph() {
            return graph;
        }

        public static class Map extends BlueprintsGraphOutputMapReduce.Map {
            @Override
            public void setup(final Mapper.Context context) throws IOException, InterruptedException {
                this.graph = TinkerGraphOutputMapReduce.getGraph();
                final String file = context.getConfiguration().get(FAUNUS_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, null);
                if (null == file)
                    this.loadingFromScratch = true;
                else {
                    this.loadingFromScratch = false;
                    final FileSystem fs = FileSystem.get(context.getConfiguration());
                    try {
                        engine.eval(new InputStreamReader(fs.open(new Path(file))));
                    } catch (Exception e) {
                        throw new IOException(e.getMessage());
                    }
                }
            }
        }

        public static class Reduce extends BlueprintsGraphOutputMapReduce.Reduce {
            @Override
            public void setup(final Reduce.Context context) throws IOException, InterruptedException {
                this.graph = TinkerGraphOutputMapReduce.getGraph();
            }
        }
    }
}

