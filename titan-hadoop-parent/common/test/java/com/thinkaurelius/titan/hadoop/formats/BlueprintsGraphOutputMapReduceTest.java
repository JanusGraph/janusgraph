package com.thinkaurelius.titan.hadoop.formats;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Holder;
import com.thinkaurelius.titan.hadoop.mapreduce.HadoopCompiler;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;

import groovy.lang.MissingMethodException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.log4j.Level;

import javax.script.ScriptException;

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

    MapReduceDriver<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>, NullWritable, HadoopVertex> vertexMapReduceDriver;
    MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex> edgeMapReduceDriver;

    public void setUp() {
        vertexMapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>, NullWritable, HadoopVertex>();
        vertexMapReduceDriver.setMapper(new TinkerGraphOutputMapReduce.VertexMap());
        vertexMapReduceDriver.setReducer(new TinkerGraphOutputMapReduce.Reduce());

        edgeMapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex>();
        edgeMapReduceDriver.setMapper(new TinkerGraphOutputMapReduce.EdgeMap());
        edgeMapReduceDriver.setReducer(new Reducer<NullWritable, HadoopVertex, NullWritable, HadoopVertex>());
    }

    public void testTinkerGraphIncrementalVertexLoading() throws Exception {
        TinkerGraphOutputMapReduce.graph = new TinkerGraph();
        Configuration conf = BlueprintsGraphOutputMapReduce.createConfiguration();
        conf.set(BlueprintsGraphOutputMapReduce.TITAN_HADOOP_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, "../data/BlueprintsScript.groovy");
        vertexMapReduceDriver.withConfiguration(conf);
        Map<Long, HadoopVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, conf), vertexMapReduceDriver);
        edgeMapReduceDriver.withConfiguration(conf);
        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
        }
        edgeMapReduceDriver.run();

        Map<Long, HadoopVertex> incrementalGraph = new HashMap<Long, HadoopVertex>();
        // VERTICES
        HadoopVertex marko1 = new HadoopVertex(conf, 11l);
        marko1.setProperty("name", "marko");
        marko1.setProperty("height", "5'11");
        HadoopVertex stephen1 = new HadoopVertex(conf, 22l);
        stephen1.setProperty("name", "stephen");
        HadoopVertex vadas1 = new HadoopVertex(conf, 33l);
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
        conf.set(BlueprintsGraphOutputMapReduce.TITAN_HADOOP_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, "../data/BlueprintsScript.groovy");


        setUp();
        vertexMapReduceDriver.withConfiguration(conf);
        graph = runWithGraph(incrementalGraph, vertexMapReduceDriver);
        edgeMapReduceDriver.withConfiguration(conf);
        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
        }
        edgeMapReduceDriver.run();

        final Graph tinkerGraph = ((TinkerGraphOutputMapReduce.VertexMap) vertexMapReduceDriver.getMapper()).graph;

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

    public void testTinkerGraphIncrementalEdgeLoading() throws Exception {
        TinkerGraphOutputMapReduce.graph = new TinkerGraph();
        Configuration conf = BlueprintsGraphOutputMapReduce.createConfiguration();
        conf.set(BlueprintsGraphOutputMapReduce.TITAN_HADOOP_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, "../data/BlueprintsScript.groovy");
        vertexMapReduceDriver.withConfiguration(conf);
        Map<Long, HadoopVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, conf), vertexMapReduceDriver);
        edgeMapReduceDriver.withConfiguration(conf);
        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
        }
        edgeMapReduceDriver.run();

        Map<Long, HadoopVertex> incrementalGraph = new HashMap<Long, HadoopVertex>();
        // VERTICES
        HadoopVertex marko1 = new HadoopVertex(conf, 11l);
        marko1.setProperty("name", "marko");
        HadoopVertex lop1 = new HadoopVertex(conf, 22l);
        lop1.setProperty("name", "lop");
        HadoopVertex vadas1 = new HadoopVertex(conf, 33l);
        vadas1.setProperty("name", "vadas");
        // EDGES
        marko1.addEdge(Direction.OUT, "created", lop1.getIdAsLong()).setProperty("since", 2009);
        marko1.addEdge(Direction.OUT, "knows", vadas1.getIdAsLong()).setProperty("since", 2008);
        lop1.addEdge(Direction.IN, "created", marko1.getIdAsLong()).setProperty("since", 2009);
        vadas1.addEdge(Direction.IN, "knows", marko1.getIdAsLong()).setProperty("since", 2008);
        incrementalGraph.put(11l, marko1);
        incrementalGraph.put(22l, lop1);
        incrementalGraph.put(33l, vadas1);
        conf = new Configuration();
        conf.set(BlueprintsGraphOutputMapReduce.TITAN_HADOOP_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, "../data/BlueprintsScript.groovy");

        setUp();
        vertexMapReduceDriver.withConfiguration(conf);
        graph = runWithGraph(incrementalGraph, vertexMapReduceDriver);
        edgeMapReduceDriver.withConfiguration(conf);
        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
        }
        edgeMapReduceDriver.run();

        final Graph tinkerGraph = ((TinkerGraphOutputMapReduce.VertexMap) vertexMapReduceDriver.getMapper()).graph;

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
        assertTrue(null != marko);
        assertTrue(null != peter);
        assertTrue(null != josh);
        assertTrue(null != vadas);
        assertTrue(null != lop);
        assertTrue(null != ripple);
        assertEquals(count, 6);

        count = 0;
        for (Edge edge : tinkerGraph.query().edges()) {
            System.out.println(edge);
            count++;
        }
        assertEquals(count, 6);

        // test marko
        assertEquals(marko.getProperty("name"), "marko");
        assertEquals(((Number) marko.getProperty("age")).intValue(), 29);
        count = 0;
        for (Edge e : marko.getEdges(Direction.OUT, "created")) {
            count++;
            assertTrue(e.getVertex(Direction.IN).getProperty("name").equals("lop"));
            assertEquals(e.getPropertyKeys().size(), 2);
            assertEquals(e.getProperty("since"), 2009);
        }
        assertEquals(count, 1);
        count = 0;
        for (Edge e : marko.getEdges(Direction.OUT, "knows")) {
            count++;
            if (e.getVertex(Direction.IN).getProperty("name").equals("vadas")) {
                assertEquals(e.getPropertyKeys().size(), 2);
                assertEquals(e.getProperty("since"), 2008);
            } else if (e.getVertex(Direction.IN).getProperty("name").equals("josh")) {
                assertEquals(e.getPropertyKeys().size(), 1);
            } else {
                assertTrue(false);
            }
        }
        assertEquals(count, 2);

        // test peter
        Set<Vertex> vertices = new HashSet<Vertex>();
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
        Configuration conf = BlueprintsGraphOutputMapReduce.createConfiguration();
        vertexMapReduceDriver.withConfiguration(conf);
        Map<Long, HadoopVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, conf), vertexMapReduceDriver);
        conf = BlueprintsGraphOutputMapReduce.createConfiguration();
        edgeMapReduceDriver.withConfiguration(conf);
        edgeMapReduceDriver.resetOutput();
        edgeMapReduceDriver.getConfiguration().setBoolean(HadoopCompiler.TESTING, true);
        assertEquals(graph.size(), 6);
        int counter = 0;
        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
            counter++;
        }
        assertEquals(counter, 6);
        counter = 0;
        for (Pair<NullWritable, HadoopVertex> entry : edgeMapReduceDriver.run()) {
            counter++;
            // THIS IS THE DEAD_VERTEX (NOTHING EMITTED TO HDFS)
            assertEquals(count(entry.getSecond().getEdges(Direction.IN)), 0);
            assertEquals(count(entry.getSecond().getEdges(Direction.OUT)), 0);
            assertEquals(entry.getSecond().getProperties().size(), 0);
            assertEquals(entry.getSecond().getIdAsLong(), -1);
        }
        assertEquals(counter, 6);

        final Graph tinkerGraph = ((TinkerGraphOutputMapReduce.VertexMap) vertexMapReduceDriver.getMapper()).graph;

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

        public static class VertexMap extends BlueprintsGraphOutputMapReduce.VertexMap {
            @Override
            public void setup(final Mapper.Context context) throws IOException, InterruptedException {
                this.graph = TinkerGraphOutputMapReduce.getGraph();
                // this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
                final String file = context.getConfiguration().get(TITAN_HADOOP_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, null);
                if (null != file && firstRead) {
                    final FileSystem fs = FileSystem.get(context.getConfiguration());
                    try {
                        engine = new GremlinGroovyScriptEngine();
                        engine.eval(new InputStreamReader(fs.open(new Path(file))));
                        try {
                            engine.eval("getOrCreateVertex(null,null,null)");
                        } catch (ScriptException se) {
                            if (se.getCause().getCause() instanceof MissingMethodException)
                                engine = null;
                        }
                    } catch (Exception e) {
                        throw new IOException(e.getMessage());
                    }
                    firstRead = false;
                }
                LOGGER.setLevel(Level.INFO);
            }
        }

        public static class Reduce extends BlueprintsGraphOutputMapReduce.Reduce {

        }

        public static class EdgeMap extends BlueprintsGraphOutputMapReduce.EdgeMap {
            @Override
            public void setup(final Mapper.Context context) throws IOException, InterruptedException {
                this.graph = TinkerGraphOutputMapReduce.getGraph();
                //this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
                final String file = context.getConfiguration().get(TITAN_HADOOP_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, null);
                if (null != file && firstRead) {
                    final FileSystem fs = FileSystem.get(context.getConfiguration());
                    try {
                        engine = new GremlinGroovyScriptEngine();
                        engine.eval(new InputStreamReader(fs.open(new Path(file))));
                        try {
                            engine.eval("getOrCreateEdge(null,null,null,null,null)");
                        } catch (ScriptException se) {
                            if (se.getCause().getCause() instanceof MissingMethodException)
                                engine = null;
                        }
                    } catch (Exception e) {
                        throw new IOException(e.getMessage());
                    }
                    firstRead = false;
                }
                LOGGER.setLevel(Level.INFO);

            }
        }
    }
}

