package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.graphson.GraphSONUtility;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import junit.framework.TestCase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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

    public static List<FaunusVertex> generateGraph(final ExampleGraph example) throws IOException {
        if (ExampleGraph.TINKERGRAPH.equals(example))
            return new GraphSONUtility().fromJSON(GraphSONUtility.class.getResourceAsStream("graph-example-1.json"));
        else
            return new GraphSONUtility().fromJSON(GraphSONUtility.class.getResourceAsStream("graph-of-the-gods.json"));
    }

    public static Map<Long, FaunusVertex> generateIndexedGraph(final ExampleGraph example) throws IOException {
        Map<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        for (FaunusVertex vertex : generateGraph(example)) {
            map.put(vertex.getIdAsLong(), vertex);
        }
        return map;
    }

    public static Collection<FaunusVertex> startPath(final Collection<FaunusVertex> vertices, final Class<? extends Element> klass) {
        return startPath(vertices, klass, false);
    }

    public static Collection<FaunusVertex> startPath(final Collection<FaunusVertex> vertices, final Class<? extends Element> klass, boolean enablePath) {
        for (FaunusVertex vertex : vertices) {
            if (klass.equals(Vertex.class)) {
                vertex.enablePath(enablePath);
                vertex.startPath();
            } else if (klass.equals(Edge.class)) {
                for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                    ((FaunusEdge) edge).enablePath(enablePath);
                    ((FaunusEdge) edge).startPath();
                }
            } else {
                startPath(vertices, Vertex.class, enablePath);
                startPath(vertices, Edge.class, enablePath);
            }
        }
        return vertices;
    }

    private static Map<Long, FaunusVertex> indexResults(final List<Pair<NullWritable, FaunusVertex>> pairs) {
        final Map<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        for (final Pair<NullWritable, FaunusVertex> pair : pairs) {
            map.put(pair.getSecond().getIdAsLong(), pair.getSecond());
        }
        return map;
    }

    public static Map<Long, FaunusVertex> runWithGraph(Collection<FaunusVertex> vertices, final MapReduceDriver driver) throws IOException {
        driver.resetOutput();
        for (final Vertex vertex : vertices) {
            driver.withInput(NullWritable.get(), vertex);
        }
        return indexResults(driver.run());
    }

    public static List runWithGraphNoIndex(Collection<FaunusVertex> vertices, final MapReduceDriver driver) throws IOException {
        driver.resetOutput();
        for (final Vertex vertex : vertices) {
            driver.withInput(NullWritable.get(), vertex);
        }
        return driver.run();
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
        Map<Long, FaunusVertex> otherVertices = generateIndexedGraph(exampleGraph);
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
            assertEquals(vertices.get(10l).getEdges(Direction.IN, "battled").iterator().next().getProperty("time"), 2);
            assertEquals(vertices.get(11l).getEdges(Direction.IN).iterator().next().getProperty("time"), 12);
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
}
