package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.json.JSONUtility;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import junit.framework.TestCase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BaseTest extends TestCase {

    public static enum ExampleGraph {GRAPH_OF_THE_GODS, TINKERGRAPH}

    public void testTrue() {
        assertTrue(true);
    }

    public static <T> List<T> asList(final Iterable<T> iterable) {
        final List<T> list = new LinkedList<T>();
        for (final T t : iterable) {
            list.add(t);
        }
        return list;
    }

    public static List<FaunusVertex> generateToyGraph(final ExampleGraph example) throws IOException {
        if (ExampleGraph.TINKERGRAPH.equals(example))
            return new JSONUtility().fromJSON(JSONUtility.class.getResourceAsStream("graph-example-1.json"));
        else
            return new JSONUtility().fromJSON(JSONUtility.class.getResourceAsStream("graph-of-the-gods.json"));
    }

    public static Map<Long, FaunusVertex> indexResults(final List<Pair<NullWritable, FaunusVertex>> pairs) {
        final Map<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        for (final Pair<NullWritable, FaunusVertex> pair : pairs) {
            map.put(pair.getSecond().getIdAsLong(), pair.getSecond());
        }
        return map;
    }

    public static Map<Long, FaunusVertex> runWithToyGraph(final ExampleGraph example, final MapReduceDriver driver) throws IOException {
        driver.resetOutput();
        for (final FaunusVertex vertex : generateToyGraph(example)) {
            driver.withInput(NullWritable.get(), vertex);
        }
        return indexResults(driver.run());
    }

    public void testConverter() throws IOException {
        Graph graph = new TinkerGraph();
        GraphMLReader.inputGraph(graph, JSONUtility.class.getResourceAsStream("graph-of-the-gods.xml"));
        //Graph graph = TinkerGraphFactory.createTinkerGraph();
        BufferedWriter bw = new BufferedWriter(new FileWriter("target/graph-example-1.json"));
        for (final Vertex vertex : graph.getVertices()) {
            bw.write(JSONUtility.toJSON(vertex).toString() + "\n");
        }
        bw.close();
    }
}
