package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.graphson.GraphSONUtility;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
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

    public static enum ExampleGraph {GRAPH_OF_THE_GODS, GRAPH_OF_THE_GODS_2, TINKERGRAPH}

    public static <T> List<T> asList(final Iterable<T> iterable) {
        final List<T> list = new ArrayList<T>();
        for (final T t : iterable) {
            list.add(t);
        }
        return list;
    }

    public static Map<Long, FaunusVertex> generateGraph(final ExampleGraph example) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setBoolean(FaunusCompiler.PATH_ENABLED, false);
        return generateGraph(example, configuration);
    }

    public static Map<Long, FaunusVertex> generateGraph(final ExampleGraph example, final Configuration configuration) throws Exception {
        final List<FaunusVertex> vertices;
        if (ExampleGraph.TINKERGRAPH.equals(example))
            vertices = new GraphSONUtility().fromJSON(GraphSONUtility.class.getResourceAsStream("graph-example-1.json"));
        else if (ExampleGraph.GRAPH_OF_THE_GODS.equals(example))
            vertices = new GraphSONUtility().fromJSON(GraphSONUtility.class.getResourceAsStream("graph-of-the-gods.json"));
        else {
            vertices = new ArrayList<FaunusVertex>();
            FaunusVertex saturn = new FaunusVertex(4l);
            vertices.add(saturn);
            saturn.setProperty("name", "saturn");
            saturn.setProperty("age", 10000);
            saturn.setProperty("type", "titan");

            FaunusVertex sky = new FaunusVertex(8l);
            vertices.add(sky);
            ElementHelper.setProperties(sky, "name", "sky", "type", "location");

            FaunusVertex sea = new FaunusVertex(12l);
            vertices.add(sea);
            ElementHelper.setProperties(sea, "name", "sea", "type", "location");

            FaunusVertex jupiter = new FaunusVertex(16l);
            vertices.add(jupiter);
            ElementHelper.setProperties(jupiter, "name", "jupiter", "age", 5000, "type", "god");

            FaunusVertex neptune = new FaunusVertex(20l);
            vertices.add(neptune);
            ElementHelper.setProperties(neptune, "name", "neptune", "age", 4500, "type", "god");

            FaunusVertex hercules = new FaunusVertex(24l);
            vertices.add(hercules);
            ElementHelper.setProperties(hercules, "name", "hercules", "age", 30, "type", "demigod");

            FaunusVertex alcmene = new FaunusVertex(28l);
            vertices.add(alcmene);
            ElementHelper.setProperties(alcmene, "name", "alcmene", "age", 45, "type", "human");

            FaunusVertex pluto = new FaunusVertex(32l);
            vertices.add(pluto);
            ElementHelper.setProperties(pluto, "name", "pluto", "age", 4000, "type", "god");

            FaunusVertex nemean = new FaunusVertex(36l);
            vertices.add(nemean);
            ElementHelper.setProperties(nemean, "name", "nemean", "type", "monster");

            FaunusVertex hydra = new FaunusVertex(40l);
            vertices.add(hydra);
            ElementHelper.setProperties(hydra, "name", "hydra", "type", "monster");

            FaunusVertex cerberus = new FaunusVertex(44l);
            vertices.add(cerberus);
            ElementHelper.setProperties(cerberus, "name", "cerberus", "type", "monster");

            FaunusVertex tartarus = new FaunusVertex(48l);
            vertices.add(tartarus);
            ElementHelper.setProperties(tartarus, "name", "tartarus", "type", "location");

            // edges

            jupiter.addEdge("father", saturn);
            jupiter.addEdge("lives", sky).setProperty("reason", "loves fresh breezes");
            jupiter.addEdge("brother", neptune);
            jupiter.addEdge("brother", pluto);

            neptune.addEdge("lives", sea).setProperty("reason", "loves waves");
            neptune.addEdge("brother", jupiter);
            neptune.addEdge("brother", pluto);

            hercules.addEdge("father", jupiter);
            hercules.addEdge("mother", alcmene);
            ElementHelper.setProperties(hercules.addEdge("battled", nemean), "time", 1, "place", Geoshape.point(38.1f, 23.7f));
            ElementHelper.setProperties(hercules.addEdge("battled", hydra), "time", 2, "place", Geoshape.point(37.7f, 23.9f));
            ElementHelper.setProperties(hercules.addEdge("battled", cerberus), "time", 12, "place", Geoshape.point(39f, 22f));

            pluto.addEdge("brother", jupiter);
            pluto.addEdge("brother", neptune);
            pluto.addEdge("lives", tartarus).setProperty("reason", "no fear of death");
            pluto.addEdge("pet", cerberus);

            cerberus.addEdge("lives", tartarus);
        }


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

    // TODO: Remove the body of this with VertexHelper.areEqual() with the release of TinkerPop 2.3.2
    public static void identicalStructure(final Map<Long, FaunusVertex> vertices, final ExampleGraph exampleGraph) throws Exception {
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
            if (exampleGraph != ExampleGraph.GRAPH_OF_THE_GODS_2) {
                assertEquals(asList(v1.getEdges(Direction.BOTH)).size(), asList(v2.getEdges(Direction.BOTH)).size());
                assertEquals(asList(v1.getEdges(Direction.IN)).size(), asList(v2.getEdges(Direction.IN)).size());
                assertEquals(asList(v1.getEdges(Direction.OUT)).size(), asList(v2.getEdges(Direction.OUT)).size());

                assertEquals(v1.getEdgeLabels(Direction.BOTH).size(), v2.getEdgeLabels(Direction.BOTH).size());
                assertEquals(v1.getEdgeLabels(Direction.IN).size(), v2.getEdgeLabels(Direction.IN).size());
                assertEquals(v1.getEdgeLabels(Direction.OUT).size(), v2.getEdgeLabels(Direction.OUT).size());
            }
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
