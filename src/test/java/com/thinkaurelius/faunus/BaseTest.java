package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.graphson.GraphSONUtility;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class BaseTest extends TestCase {

    public static enum ExampleGraph {GRAPH_OF_THE_GODS, TINKERGRAPH}

    public static <T> List<T> asList(final Iterable<T> iterable) {
        final List<T> list = new ArrayList<T>();
        for (final T t : iterable) {
            list.add(t);
        }
        return list;
    }

    public static Map<Long, FaunusVertex> generateGraph(final ExampleGraph example) throws IOException {
        Configuration configuration = new Configuration();
        configuration.setBoolean(FaunusCompiler.PATH_ENABLED, false);
        return generateGraph(example, configuration);
    }

    public static Map<Long, FaunusVertex> generateGraph(final ExampleGraph example, final Configuration configuration) throws IOException {
        final List<FaunusVertex> vertices;
        if (ExampleGraph.TINKERGRAPH.equals(example))
            vertices = new GraphSONUtility().fromJSON(GraphSONUtility.class.getResourceAsStream("graph-example-1.json"));
        else
            vertices = new GraphSONUtility().fromJSON(GraphSONUtility.class.getResourceAsStream("graph-of-the-gods.json"));

        for (final FaunusVertex vertex : vertices) {
            vertex.enablePath(configuration.getBoolean(FaunusCompiler.PATH_ENABLED, false));
            for (final Edge edge : vertex.getEdges(Direction.BOTH)) {
                ((FaunusEdge) edge).enablePath(configuration.getBoolean(FaunusCompiler.PATH_ENABLED, false));
            }
        }

        final Map<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        for (final FaunusVertex vertex : vertices) {
            map.put(vertex.getIdAsLong(), vertex);
        }
        return map;
    }

    public static Map<Long, FaunusVertex> startPath(final Map<Long, FaunusVertex> graph, final Class<? extends Element> klass, final long... ids) {
        if (ids.length == 0) {
            for (FaunusVertex vertex : graph.values()) {
                if (klass.equals(Vertex.class)) {
                    vertex.startPath();
                } else if (klass.equals(Edge.class)) {
                    for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                        ((FaunusEdge) edge).startPath();
                    }
                } else {
                    throw new IllegalArgumentException("It can only be either edge or vertex, not both");
                }
            }
        } else {
            if (klass.equals(Edge.class))
                throw new IllegalArgumentException("Currently no support for starting a path on a particular set of edges (only all edges)");

            for (long id : ids) {
                if (graph.get(id).hasPaths())
                    graph.get(id).incrPath(1);
                else
                    graph.get(id).startPath();
            }
        }
        return graph;
    }

    public static Map<Long, FaunusVertex> runWithGraph(final Map<Long, FaunusVertex> graph, final MapReduceDriver driver) throws IOException {
        driver.resetOutput();
        driver.getConfiguration().setBoolean(FaunusCompiler.TESTING, true);
        for (final FaunusVertex vertex : graph.values()) {
            driver.withInput(NullWritable.get(), vertex);
        }

        final Map<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        for (final Object pair : driver.run()) {
            map.put(((Pair<NullWritable, FaunusVertex>) pair).getSecond().getIdAsLong(), ((Pair<NullWritable, FaunusVertex>) pair).getSecond());
        }
        return map;
    }

    public static List runWithGraphNoIndex(final Map<Long, FaunusVertex> graph, final MapReduceDriver driver) throws IOException {
        driver.resetOutput();
        driver.getConfiguration().setBoolean(FaunusCompiler.TESTING, true);
        for (final Vertex vertex : graph.values()) {
            driver.withInput(NullWritable.get(), vertex);
        }
        return driver.run();
    }

    public static Map<Long, FaunusVertex> run(final MapReduceDriver driver) throws IOException {
        final Map<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        for (final Object object : driver.run()) {
            Pair<NullWritable, FaunusVertex> pair = (Pair<NullWritable, FaunusVertex>) object;
            map.put(pair.getSecond().getIdAsLong(), pair.getSecond());
        }
        return map;
    }

    public static String getFullString(final Vertex vertex) {
        String string = vertex.toString() + "IN[";
        for (Edge edge : vertex.getEdges(Direction.IN)) {
            string = string + edge.toString();
        }
        string = string + "]OUT[";
        for (Edge edge : vertex.getEdges(Direction.OUT)) {
            string = string + edge.toString();
        }
        return string + "]";
    }

    public static void identicalStructure(final Map<Long, FaunusVertex> vertices, final ExampleGraph exampleGraph) throws IOException {
        Map<Long, FaunusVertex> otherVertices = generateGraph(exampleGraph, new Configuration());
        assertEquals(vertices.size(), otherVertices.size());
        for (long id : vertices.keySet()) {
            assertNotNull(otherVertices.get(id));
            FaunusVertex v1 = vertices.get(id);
            FaunusVertex v2 = otherVertices.get(id);
            assertEquals(v1.getProperties().size(), v2.getProperties().size());
            for (final String key : v1.getPropertyKeys()) {
                assertEquals(v1.getProperty(key), v2.getProperty(key));
            }
            assertEquals(asList(v1.getEdges(Direction.BOTH)).size(), asList(v2.getEdges(Direction.BOTH)).size());
            assertEquals(asList(v1.getEdges(Direction.IN)).size(), asList(v2.getEdges(Direction.IN)).size());
            assertEquals(asList(v1.getEdges(Direction.OUT)).size(), asList(v2.getEdges(Direction.OUT)).size());

            assertEquals(v1.getEdgeLabels(Direction.BOTH).size(), v2.getEdgeLabels(Direction.BOTH).size());
            assertEquals(v1.getEdgeLabels(Direction.IN).size(), v2.getEdgeLabels(Direction.IN).size());
            assertEquals(v1.getEdgeLabels(Direction.OUT).size(), v2.getEdgeLabels(Direction.OUT).size());
        }

        if (exampleGraph.equals(ExampleGraph.TINKERGRAPH)) {
            assertEquals(vertices.get(1l).getEdges(Direction.OUT, "knows").iterator().next().getProperty("weight"), 0.5);
            assertEquals(vertices.get(2l).getEdges(Direction.IN, "knows").iterator().next().getProperty("weight"), 0.5);
            assertEquals(vertices.get(6l).getEdges(Direction.OUT).iterator().next().getProperty("weight"), 0.2);
            assertEquals(vertices.get(5l).getEdges(Direction.IN).iterator().next().getProperty("weight"), 1);
        } else if (exampleGraph.equals(ExampleGraph.GRAPH_OF_THE_GODS)) {
            assertEquals(vertices.get(9l).getEdges(Direction.IN, "battled").iterator().next().getProperty("time"), 1);
            assertEquals(vertices.get(10l).getEdges(Direction.IN).iterator().next().getProperty("time"), 2);
            assertEquals(asList(vertices.get(11l).getEdges(Direction.IN)).size(), 2);
            assertEquals(vertices.get(11l).getEdges(Direction.IN, "battled").iterator().next().getProperty("time"), 12);
        }
    }

    /*public void testConverter() throws IOException {
        //Graph graph = new TinkerGraph();
        //GraphMLReader.inputGraph(graph, JSONUtility.class.getResourceAsStream("graph-of-the-gods.xml"));
        Graph graph = TinkerGraphFactory.createTinkerGraph();
        BufferedWriter bw = new BufferedWriter(new FileWriter("target/graph-example-1.json"));
        for (final Vertex vertex : graph.getVertices()) {
            bw.write(JSONUtility.toJSON(vertex).toString() + "\n");
        }
        bw.close();
    }*/

    public File computeTestDataRoot() {
        final String clsUri = this.getClass().getName().replace('.', '/') + ".class";
        final URL url = this.getClass().getClassLoader().getResource(clsUri);
        final String clsPath = url.getPath();
        final File root = new File(clsPath.substring(0, clsPath.length() - clsUri.length()));
        final File temp = new File(root.getParentFile(), "test-data");
        if (!temp.exists())
            temp.mkdir();
        return temp;
    }

    public static void noPaths(final Map<Long, FaunusVertex> graph, final Class<? extends Element> klass) {
        for (FaunusVertex vertex : graph.values()) {
            if (klass.equals(Vertex.class)) {
                assertFalse(vertex.hasPaths());
                assertEquals(vertex.pathCount(), 0);
            } else {
                for (Edge e : vertex.getEdges(Direction.BOTH)) {
                    FaunusEdge edge = (FaunusEdge) e;
                    assertFalse(edge.hasPaths());
                    assertEquals(edge.pathCount(), 0);
                }
            }
        }
    }

    public static long count(final Iterable iterable) {
        long counter = 0;
        for (Object o : iterable) {
            counter++;
        }
        return counter;
    }
}
