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

    public static List<FaunusVertex> generateToyGraph(final ExampleGraph example) throws IOException {
        if (ExampleGraph.TINKERGRAPH.equals(example))
            return new GraphSONUtility().fromJSON(GraphSONUtility.class.getResourceAsStream("graph-example-1.json"));
        else
            return new GraphSONUtility().fromJSON(GraphSONUtility.class.getResourceAsStream("graph-of-the-gods.json"));
    }

    public static Map<Long, FaunusVertex> generateIndexedToyGraph(final ExampleGraph example) throws IOException {
        Map<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        for (FaunusVertex vertex : generateToyGraph(example)) {
            map.put(vertex.getIdAsLong(), vertex);
        }
        return map;
    }

    public static void incrPath(final Collection<FaunusVertex> vertices, final Class<? extends Element> klass) {
        for (FaunusVertex vertex : vertices) {
            if (klass.equals(Vertex.class))
                vertex.incrPath();
            else {
                for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                    ((FaunusEdge) edge).incrPath();
                }
            }
        }
    }


    public static Map<Long, FaunusVertex> indexResults(final List<Pair<NullWritable, FaunusVertex>> pairs) {
        final Map<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        for (final Pair<NullWritable, FaunusVertex> pair : pairs) {
            map.put(pair.getSecond().getIdAsLong(), pair.getSecond());
        }
        return map;
    }

    public static Map<Long, FaunusVertex> runWithToyGraph(final ExampleGraph example,
                                                          final MapReduceDriver driver) throws IOException {
        driver.resetOutput();
        for (final FaunusVertex vertex : generateToyGraph(example)) {
            vertex.incrPath();
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                ((FaunusEdge) edge).incrPath();
            }
            driver.withInput(NullWritable.get(), vertex);
        }
        return indexResults(driver.run());
    }

    public static Map<Long, FaunusVertex> runWithGraph(Collection<FaunusVertex> vertices,
                                                       final MapReduceDriver driver) throws IOException {
        driver.resetOutput();
        for (final Vertex vertex : vertices) {
            driver.withInput(NullWritable.get(), vertex);
        }
        return indexResults(driver.run());
    }

    public static List runWithToyGraphNoFormatting(final ExampleGraph example, final MapReduceDriver driver) throws IOException {
        driver.resetOutput();
        for (final FaunusVertex vertex : generateToyGraph(example)) {
            vertex.incrPath();
            for (Edge edge : vertex.getEdges(Direction.BOTH)) {
                ((FaunusEdge) edge).incrPath();
            }
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
