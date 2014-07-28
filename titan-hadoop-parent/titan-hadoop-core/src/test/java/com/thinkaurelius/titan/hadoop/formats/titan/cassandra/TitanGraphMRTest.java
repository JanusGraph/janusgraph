package com.thinkaurelius.titan.hadoop.formats.titan.cassandra;

import com.thinkaurelius.titan.hadoop.*;

import com.thinkaurelius.titan.hadoop.formats.TitanOutputFormatTest;
import com.thinkaurelius.titan.hadoop.FaunusVertex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanGraphMRTest extends TitanOutputFormatTest {

    MapReduceDriver<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> vertexMapReduceDriver;
    MapReduceDriver<NullWritable, FaunusVertex, NullWritable, FaunusVertex, NullWritable, FaunusVertex> edgeMapReduceDriver;

//    private static TitanGraph startUpCassandra() throws Exception {
//        ModifiableConfiguration configuration = GraphDatabaseConfiguration.buildConfiguration();
//        configuration.set(STORAGE_BACKEND,"embeddedcassandra");
//        configuration.set(STORAGE_HOSTS,new String[]{"localhost"});
//        configuration.set(STORAGE_CONF_FILE, TitanCassandraOutputFormat.class.getResource("cassandra.yaml").toString());
//        configuration.set(DB_CACHE, false);
//        configuration.set(ExpectedValueCheckingStore.LOCK_LOCAL_MEDIATOR_GROUP, "tmp");
//        configuration.set(UNIQUE_INSTANCE_ID, "inst");
//        Backend backend = new Backend(configuration);
//        backend.initialize(configuration);
//        backend.clearStorage();
//
//        return TitanFactory.open(configuration);
//    }

//    public void setUp() {
//        vertexMapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, LongWritable, Holder<HadoopVertex>, NullWritable, HadoopVertex>();
//        vertexMapReduceDriver.setMapper(new TitanGraphOutputMapReduce.VertexMap());
//        vertexMapReduceDriver.setReducer(new TitanGraphOutputMapReduce.Reduce());
//
//        edgeMapReduceDriver = new MapReduceDriver<NullWritable, HadoopVertex, NullWritable, HadoopVertex, NullWritable, HadoopVertex>();
//        edgeMapReduceDriver.setMapper(new TitanGraphOutputMapReduce.EdgeMap());
//        edgeMapReduceDriver.setReducer(new Reducer<NullWritable, HadoopVertex, NullWritable, HadoopVertex>());
//    }
//
//    private void createSchema() {
//        Configuration conf = TitanGraphOutputMapReduce.createConfiguration();
//        conf.setClass(HadoopGraph.TITAN_HADOOP_GRAPH_OUTPUT_FORMAT, TitanCassandraOutputFormat.class, OutputFormat.class);
//        final TitanGraph titanGraph = (TitanGraph)TitanGraphOutputMapReduce.generateGraph(conf);
//        TitanManagement mgmt = titanGraph.getManagementSystem();
//        PropertyKey pkey = mgmt.makePropertyKey(TitanGraphOutputMapReduce.TITAN_ID).cardinality(Cardinality.SINGLE).dataType(Long.class).make();
//        mgmt.buildIndex(TitanGraphOutputMapReduce.TITAN_ID + "_index", Vertex.class).indexKey(pkey).unique();
//        mgmt.commit();
//        titanGraph.shutdown();
//    }
//
//    public void testTinkerGraphIncrementalVertexLoading() throws Exception {
//
//        createSchema();
//
//
//        Configuration conf = TitanGraphOutputMapReduce.createConfiguration();
//        conf.setClass(HadoopGraph.TITAN_HADOOP_GRAPH_OUTPUT_FORMAT, TitanCassandraOutputFormat.class, OutputFormat.class);
//        vertexMapReduceDriver.withConfiguration(conf);
//
//        Map<Long, HadoopVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, conf), vertexMapReduceDriver);
//        edgeMapReduceDriver.withConfiguration(conf);
//        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
//            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
//        }
//        edgeMapReduceDriver.run();
//
//        TitanGraph titanGraph = (TitanGraph) TitanGraphOutputMapReduce.generateGraph(conf);
//        long markoId = ((Long)Iterables.getOnlyElement(titanGraph.getVertices("name", "marko")).getId()).longValue();
//        long vadasId = ((Long)Iterables.getOnlyElement(titanGraph.getVertices("name", "vadas")).getId()).longValue();
//        // a vertex w/ name=stephen is not in the tinkergraph example data, so we'll make a vertex for him
//        titanGraph.shutdown();
//
//        Map<Long, HadoopVertex> incrementalGraph = new HashMap<Long, HadoopVertex>();
//        // VERTICES
//        HadoopVertex marko1 = new HadoopVertex(conf, markoId);
//        marko1.setProperty("name", "marko");
//        marko1.setProperty("height", "5'11");
//        marko1.setState(ElementState.LOADED);
//        HadoopVertex stephen1 = new HadoopVertex(conf, -1L);
//        stephen1.setProperty("name", "stephen");
//        // stephen vertex will be created since default ElementState is NEW
//        HadoopVertex vadas1 = new HadoopVertex(conf, vadasId);
//        vadas1.setState(ElementState.LOADED);
//        vadas1.setProperty("name", "vadas");
//        // EDGES
//        marko1.addEdge(Direction.OUT, "worksWith", stephen1.getIdAsLong());
//        stephen1.addEdge(Direction.IN, "worksWith", marko1.getIdAsLong());
//        marko1.addEdge(Direction.OUT, "worksWith", vadas1.getIdAsLong());
//        vadas1.addEdge(Direction.IN, "worksWith", marko1.getIdAsLong());
//        stephen1.addEdge(Direction.OUT, "worksWith", vadas1.getIdAsLong());
//        vadas1.addEdge(Direction.IN, "worksWith", stephen1.getIdAsLong());
//        incrementalGraph.put(markoId, marko1);
//        incrementalGraph.put(-1L, stephen1);
//        incrementalGraph.put(vadasId, vadas1);
//
//        //conf = new Configuration();
//
//        setUp();
//        vertexMapReduceDriver.withConfiguration(conf);
//        graph = runWithGraph(incrementalGraph, vertexMapReduceDriver);
//        edgeMapReduceDriver.withConfiguration(conf);
//        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
//            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
//        }
//        edgeMapReduceDriver.run();
//
//        titanGraph = (TitanGraph)TitanGraphOutputMapReduce.generateGraph(conf);
//        Vertex marko = null;
//        Vertex peter = null;
//        Vertex josh = null;
//        Vertex vadas = null;
//        Vertex lop = null;
//        Vertex ripple = null;
//        Vertex stephen = null;
//        int count = 0;
//        for (Vertex v : titanGraph.getVertices()) {
//            count++;
//            String name = v.getProperty("name").toString();
//
//            System.out.println("Vertex " + v.getId() + " -> " + name);
//
//            if (name.equals("marko")) {
//                marko = v;
//            } else if (name.equals("peter")) {
//                peter = v;
//            } else if (name.equals("josh")) {
//                josh = v;
//            } else if (name.equals("vadas")) {
//                vadas = v;
//            } else if (name.equals("lop")) {
//                lop = v;
//            } else if (name.equals("ripple")) {
//                ripple = v;
//            } else if (name.equals("stephen")) {
//                stephen = v;
//            } else {
//                assertTrue(false);
//            }
//        }
//        assertTrue(null != marko);
//        assertTrue(null != peter);
//        assertTrue(null != josh);
//        assertTrue(null != vadas);
//        assertTrue(null != lop);
//        assertTrue(null != ripple);
//        assertTrue(null != stephen);
//        assertEquals(7, count);
//
//        Set<Vertex> vertices = new HashSet<Vertex>();
//
//        // test marko
//        count = 0;
//        for (Vertex v : marko.getVertices(Direction.OUT, "worksWith")) {
//            count++;
//            assertTrue(v.getProperty("name").equals("stephen") || v.getProperty("name").equals("vadas"));
//        }
//        assertEquals(2, count);
//        assertEquals("marko", marko.getProperty("name"));
//        assertEquals(29, ((Number) marko.getProperty("age")).intValue());
//        assertEquals("5'11", marko.getProperty("height"));
//        assertEquals(3, marko.getPropertyKeys().size());
//
//        // test stephen
//        count = 0;
//        for (Vertex v : stephen.getVertices(Direction.OUT, "worksWith")) {
//            count++;
//            assertEquals(v.getProperty("name"), "vadas");
//        }
//        assertEquals(count, 1);
//        count = 0;
//        for (Vertex v : stephen.getVertices(Direction.IN, "worksWith")) {
//            count++;
//            assertEquals(v.getProperty("name"), "marko");
//        }
//        assertEquals(count, 1);
//        assertEquals(stephen.getProperty("name"), "stephen");
//        assertEquals(stephen.getPropertyKeys().size(), 1);
//
//        // test peter
//        vertices = new HashSet<Vertex>();
//        assertEquals(peter.getProperty("name"), "peter");
//        assertEquals(((Number) peter.getProperty("age")).intValue(), 35);
//        assertEquals(peter.getPropertyKeys().size(), 2);
//        assertEquals(count(peter.getEdges(Direction.OUT)), 1);
//        assertEquals(count(peter.getEdges(Direction.IN)), 0);
//        for (Edge e : peter.getEdges(Direction.OUT)) {
//            vertices.add(e.getVertex(Direction.IN));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//            assertEquals(e.getProperty("weight"), 0.2);
//        }
//        assertEquals(vertices.size(), 1);
//        assertTrue(vertices.contains(lop));
//        // test ripple
//        vertices = new HashSet<Vertex>();
//        assertEquals(ripple.getProperty("name"), "ripple");
//        assertEquals(ripple.getProperty("lang"), "java");
//        assertEquals(ripple.getPropertyKeys().size(), 2);
//        assertEquals(count(ripple.getEdges(Direction.OUT)), 0);
//        assertEquals(count(ripple.getEdges(Direction.IN)), 1);
//        for (Edge e : ripple.getEdges(Direction.IN)) {
//            vertices.add(e.getVertex(Direction.OUT));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//            assertEquals(e.getProperty("weight"), 1);
//        }
//        assertEquals(vertices.size(), 1);
//        assertTrue(vertices.contains(josh));
//    }

//    public void testTinkerGraphIncrementalEdgeLoading() throws Exception {
//        TinkerGraphOutputMapReduce.graph = new TinkerGraph();
//        Configuration conf = TitanGraphOutputMapReduce.createConfiguration();
//        conf.set(TitanGraphOutputMapReduce.TITAN_HADOOP_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, "../data/BlueprintsScript.groovy");
//        vertexMapReduceDriver.withConfiguration(conf);
//        Map<Long, HadoopVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, conf), vertexMapReduceDriver);
//        edgeMapReduceDriver.withConfiguration(conf);
//        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
//            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
//        }
//        edgeMapReduceDriver.run();
//
//        Map<Long, HadoopVertex> incrementalGraph = new HashMap<Long, HadoopVertex>();
//        // VERTICES
//        HadoopVertex marko1 = new HadoopVertex(conf, 11l);
//        marko1.setProperty("name", "marko");
//        HadoopVertex lop1 = new HadoopVertex(conf, 22l);
//        lop1.setProperty("name", "lop");
//        HadoopVertex vadas1 = new HadoopVertex(conf, 33l);
//        vadas1.setProperty("name", "vadas");
//        // EDGES
//        marko1.addEdge(Direction.OUT, "created", lop1.getIdAsLong()).setProperty("since", 2009);
//        marko1.addEdge(Direction.OUT, "knows", vadas1.getIdAsLong()).setProperty("since", 2008);
//        lop1.addEdge(Direction.IN, "created", marko1.getIdAsLong()).setProperty("since", 2009);
//        vadas1.addEdge(Direction.IN, "knows", marko1.getIdAsLong()).setProperty("since", 2008);
//        incrementalGraph.put(11l, marko1);
//        incrementalGraph.put(22l, lop1);
//        incrementalGraph.put(33l, vadas1);
//        conf = new Configuration();
//        conf.set(TitanGraphOutputMapReduce.TITAN_HADOOP_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, "../data/BlueprintsScript.groovy");
//
//        setUp();
//        vertexMapReduceDriver.withConfiguration(conf);
//        graph = runWithGraph(incrementalGraph, vertexMapReduceDriver);
//        edgeMapReduceDriver.withConfiguration(conf);
//        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
//            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
//        }
//        edgeMapReduceDriver.run();
//
//        final Graph tinkerGraph = ((TinkerGraphOutputMapReduce.VertexMap) vertexMapReduceDriver.getMapper()).graph;
//
//        Vertex marko = null;
//        Vertex peter = null;
//        Vertex josh = null;
//        Vertex vadas = null;
//        Vertex lop = null;
//        Vertex ripple = null;
//        int count = 0;
//        for (Vertex v : tinkerGraph.getVertices()) {
//            count++;
//            String name = v.getProperty("name").toString();
//            if (name.equals("marko")) {
//                marko = v;
//            } else if (name.equals("peter")) {
//                peter = v;
//            } else if (name.equals("josh")) {
//                josh = v;
//            } else if (name.equals("vadas")) {
//                vadas = v;
//            } else if (name.equals("lop")) {
//                lop = v;
//            } else if (name.equals("ripple")) {
//                ripple = v;
//            } else {
//                assertTrue(false);
//            }
//        }
//        assertTrue(null != marko);
//        assertTrue(null != peter);
//        assertTrue(null != josh);
//        assertTrue(null != vadas);
//        assertTrue(null != lop);
//        assertTrue(null != ripple);
//        assertEquals(count, 6);
//
//        count = 0;
//        for (Edge edge : tinkerGraph.query().edges()) {
//            System.out.println(edge);
//            count++;
//        }
//        assertEquals(count, 6);
//
//        // test marko
//        assertEquals(marko.getProperty("name"), "marko");
//        assertEquals(((Number) marko.getProperty("age")).intValue(), 29);
//        count = 0;
//        for (Edge e : marko.getEdges(Direction.OUT, "created")) {
//            count++;
//            assertTrue(e.getVertex(Direction.IN).getProperty("name").equals("lop"));
//            assertEquals(e.getPropertyKeys().size(), 2);
//            assertEquals(e.getProperty("since"), 2009);
//        }
//        assertEquals(count, 1);
//        count = 0;
//        for (Edge e : marko.getEdges(Direction.OUT, "knows")) {
//            count++;
//            if (e.getVertex(Direction.IN).getProperty("name").equals("vadas")) {
//                assertEquals(e.getPropertyKeys().size(), 2);
//                assertEquals(e.getProperty("since"), 2008);
//            } else if (e.getVertex(Direction.IN).getProperty("name").equals("josh")) {
//                assertEquals(e.getPropertyKeys().size(), 1);
//            } else {
//                assertTrue(false);
//            }
//        }
//        assertEquals(count, 2);
//
//        // test peter
//        Set<Vertex> vertices = new HashSet<Vertex>();
//        assertEquals(peter.getProperty("name"), "peter");
//        assertEquals(((Number) peter.getProperty("age")).intValue(), 35);
//        assertEquals(peter.getPropertyKeys().size(), 2);
//        assertEquals(count(peter.getEdges(Direction.OUT)), 1);
//        assertEquals(count(peter.getEdges(Direction.IN)), 0);
//        for (Edge e : peter.getEdges(Direction.OUT)) {
//            vertices.add(e.getVertex(Direction.IN));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//            assertEquals(e.getProperty("weight"), 0.2);
//        }
//        assertEquals(vertices.size(), 1);
//        assertTrue(vertices.contains(lop));
//        // test ripple
//        vertices = new HashSet<Vertex>();
//        assertEquals(ripple.getProperty("name"), "ripple");
//        assertEquals(ripple.getProperty("lang"), "java");
//        assertEquals(ripple.getPropertyKeys().size(), 2);
//        assertEquals(count(ripple.getEdges(Direction.OUT)), 0);
//        assertEquals(count(ripple.getEdges(Direction.IN)), 1);
//        for (Edge e : ripple.getEdges(Direction.IN)) {
//            vertices.add(e.getVertex(Direction.OUT));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//            assertEquals(e.getProperty("weight"), 1);
//        }
//        assertEquals(vertices.size(), 1);
//        assertTrue(vertices.contains(josh));
//    }
//
//
//    public void testTinkerGraphMapping() throws Exception {
//        TinkerGraphOutputMapReduce.graph = new TinkerGraph();
//        Configuration conf = TitanGraphOutputMapReduce.createConfiguration();
//        vertexMapReduceDriver.withConfiguration(conf);
//        Map<Long, HadoopVertex> graph = runWithGraph(generateGraph(BaseTest.ExampleGraph.TINKERGRAPH, conf), vertexMapReduceDriver);
//        conf = TitanGraphOutputMapReduce.createConfiguration();
//        edgeMapReduceDriver.withConfiguration(conf);
//        edgeMapReduceDriver.resetOutput();
//        edgeMapReduceDriver.getConfiguration().setBoolean(HadoopCompiler.TESTING, true);
//        assertEquals(graph.size(), 6);
//        int counter = 0;
//        for (Map.Entry<Long, HadoopVertex> entry : graph.entrySet()) {
//            edgeMapReduceDriver.withInput(NullWritable.get(), entry.getValue());
//            counter++;
//        }
//        assertEquals(counter, 6);
//        counter = 0;
//        for (Pair<NullWritable, HadoopVertex> entry : edgeMapReduceDriver.run()) {
//            counter++;
//            // THIS IS THE DEAD_VERTEX (NOTHING EMITTED TO HDFS)
//            assertEquals(count(entry.getSecond().getEdges(Direction.IN)), 0);
//            assertEquals(count(entry.getSecond().getEdges(Direction.OUT)), 0);
//            assertEquals(entry.getSecond().getProperties().size(), 0);
//            assertEquals(entry.getSecond().getIdAsLong(), -1);
//        }
//        assertEquals(counter, 6);
//
//        final Graph tinkerGraph = ((TinkerGraphOutputMapReduce.VertexMap) vertexMapReduceDriver.getMapper()).graph;
//
//        Vertex marko = null;
//        Vertex peter = null;
//        Vertex josh = null;
//        Vertex vadas = null;
//        Vertex lop = null;
//        Vertex ripple = null;
//        int count = 0;
//        for (Vertex v : tinkerGraph.getVertices()) {
//            count++;
//            String name = v.getProperty("name").toString();
//            if (name.equals("marko")) {
//                marko = v;
//            } else if (name.equals("peter")) {
//                peter = v;
//            } else if (name.equals("josh")) {
//                josh = v;
//            } else if (name.equals("vadas")) {
//                vadas = v;
//            } else if (name.equals("lop")) {
//                lop = v;
//            } else if (name.equals("ripple")) {
//                ripple = v;
//            } else {
//                assertTrue(false);
//            }
//        }
//        assertEquals(count, 6);
//        assertTrue(null != marko);
//        assertTrue(null != peter);
//        assertTrue(null != josh);
//        assertTrue(null != vadas);
//        assertTrue(null != lop);
//        assertTrue(null != ripple);
//
//        assertEquals(count(tinkerGraph.getEdges()), 6);
//
//        // test marko
//        Set<Vertex> vertices = new HashSet<Vertex>();
//        assertEquals(marko.getProperty("name"), "marko");
//        assertEquals(((Number) marko.getProperty("age")).intValue(), 29);
//        assertEquals(marko.getPropertyKeys().size(), 2);
//        assertEquals(count(marko.getEdges(Direction.OUT)), 3);
//        assertEquals(count(marko.getEdges(Direction.IN)), 0);
//        for (Edge e : marko.getEdges(Direction.OUT)) {
//            vertices.add(e.getVertex(Direction.IN));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//        }
//        assertEquals(vertices.size(), 3);
//        assertTrue(vertices.contains(lop));
//        assertTrue(vertices.contains(josh));
//        assertTrue(vertices.contains(vadas));
//        // test peter
//        vertices = new HashSet<Vertex>();
//        assertEquals(peter.getProperty("name"), "peter");
//        assertEquals(((Number) peter.getProperty("age")).intValue(), 35);
//        assertEquals(peter.getPropertyKeys().size(), 2);
//        assertEquals(count(peter.getEdges(Direction.OUT)), 1);
//        assertEquals(count(peter.getEdges(Direction.IN)), 0);
//        for (Edge e : peter.getEdges(Direction.OUT)) {
//            vertices.add(e.getVertex(Direction.IN));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//            assertEquals(e.getProperty("weight"), 0.2);
//        }
//        assertEquals(vertices.size(), 1);
//        assertTrue(vertices.contains(lop));
//        // test josh
//        vertices = new HashSet<Vertex>();
//        assertEquals(josh.getProperty("name"), "josh");
//        assertEquals(((Number) josh.getProperty("age")).intValue(), 32);
//        assertEquals(josh.getPropertyKeys().size(), 2);
//        assertEquals(count(josh.getEdges(Direction.OUT)), 2);
//        assertEquals(count(josh.getEdges(Direction.IN)), 1);
//        for (Edge e : josh.getEdges(Direction.OUT)) {
//            vertices.add(e.getVertex(Direction.IN));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//        }
//        assertEquals(vertices.size(), 2);
//        assertTrue(vertices.contains(lop));
//        assertTrue(vertices.contains(ripple));
//        vertices = new HashSet<Vertex>();
//        for (Edge e : josh.getEdges(Direction.IN)) {
//            vertices.add(e.getVertex(Direction.OUT));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//            assertEquals(e.getProperty("weight"), 1);
//        }
//        assertEquals(vertices.size(), 1);
//        assertTrue(vertices.contains(marko));
//        // test vadas
//        vertices = new HashSet<Vertex>();
//        assertEquals(vadas.getProperty("name"), "vadas");
//        assertEquals(((Number) vadas.getProperty("age")).intValue(), 27);
//        assertEquals(vadas.getPropertyKeys().size(), 2);
//        assertEquals(count(vadas.getEdges(Direction.OUT)), 0);
//        assertEquals(count(vadas.getEdges(Direction.IN)), 1);
//        for (Edge e : vadas.getEdges(Direction.IN)) {
//            vertices.add(e.getVertex(Direction.OUT));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//            assertEquals(e.getProperty("weight"), 0.5);
//        }
//        assertEquals(vertices.size(), 1);
//        assertTrue(vertices.contains(marko));
//        // test lop
//        vertices = new HashSet<Vertex>();
//        assertEquals(lop.getProperty("name"), "lop");
//        assertEquals(lop.getProperty("lang"), "java");
//        assertEquals(lop.getPropertyKeys().size(), 2);
//        assertEquals(count(lop.getEdges(Direction.OUT)), 0);
//        assertEquals(count(lop.getEdges(Direction.IN)), 3);
//        for (Edge e : lop.getEdges(Direction.IN)) {
//            vertices.add(e.getVertex(Direction.OUT));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//        }
//        assertEquals(vertices.size(), 3);
//        assertTrue(vertices.contains(marko));
//        assertTrue(vertices.contains(josh));
//        assertTrue(vertices.contains(peter));
//        // test ripple
//        vertices = new HashSet<Vertex>();
//        assertEquals(ripple.getProperty("name"), "ripple");
//        assertEquals(ripple.getProperty("lang"), "java");
//        assertEquals(ripple.getPropertyKeys().size(), 2);
//        assertEquals(count(ripple.getEdges(Direction.OUT)), 0);
//        assertEquals(count(ripple.getEdges(Direction.IN)), 1);
//        for (Edge e : ripple.getEdges(Direction.IN)) {
//            vertices.add(e.getVertex(Direction.OUT));
//            assertEquals(e.getPropertyKeys().size(), 1);
//            assertNotNull(e.getProperty("weight"));
//            assertEquals(e.getProperty("weight"), 1);
//        }
//        assertEquals(vertices.size(), 1);
//        assertTrue(vertices.contains(josh));
//    }
}
