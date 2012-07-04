package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.io.formats.json.FaunusJSONParser;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import junit.framework.TestCase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BaseTest extends TestCase {

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

    public static List<FaunusVertex> generateToyGraph() throws IOException {
        return new FaunusJSONParser().parse(FaunusJSONParser.class.getResourceAsStream("graph-example-1.json"));
    }

    public static Map<Long, FaunusVertex> indexResults(final List<Pair<NullWritable, FaunusVertex>> pairs) {
        final Map<Long, FaunusVertex> map = new HashMap<Long, FaunusVertex>();
        for (final Pair<NullWritable, FaunusVertex> pair : pairs) {
            map.put(pair.getSecond().getIdAsLong(), pair.getSecond());
        }
        return map;
    }

    public static Map<Long, FaunusVertex> runWithToyGraph(final MapReduceDriver driver) throws IOException {
        driver.resetOutput();
        for (final FaunusVertex vertex : generateToyGraph()) {
            driver.withInput(NullWritable.get(), vertex);
        }
        return indexResults(driver.run());
    }

    public static Map<Long, FaunusVertex> runWithToyGraph(final MapDriver driver) throws IOException {
        driver.resetOutput();
        for (final FaunusVertex vertex : generateToyGraph()) {
            driver.withInput(NullWritable.get(), vertex);
        }
        return indexResults(driver.run());
    }
}
