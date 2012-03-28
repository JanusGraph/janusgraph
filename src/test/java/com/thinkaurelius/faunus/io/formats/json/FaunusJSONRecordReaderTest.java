package com.thinkaurelius.faunus.io.formats.json;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.pgm.Edge;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusJSONRecordReaderTest extends TestCase {

    public void testParser1() throws IOException {
        FaunusJSONRecordReader reader = new FaunusJSONRecordReader();
        FaunusVertex vertex = reader.parseVertex("{\"id\":1}");
        assertEquals(vertex.getId(), 1l);
        assertFalse(vertex.getOutEdges().iterator().hasNext());

    }

    public void testParser2() throws IOException {
        FaunusJSONRecordReader reader = new FaunusJSONRecordReader();
        FaunusVertex vertex = reader.parseVertex("{\"id\":1, \"properties\":{\"name\":\"marko\",\"age\":32}}");
        assertEquals(vertex.getId(), 1l);
        assertFalse(vertex.getOutEdges().iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getProperty("age"), 32l);
    }

    public void testParser3() throws IOException {
        FaunusJSONRecordReader reader = new FaunusJSONRecordReader();
        FaunusVertex vertex = reader.parseVertex("{\"id\":1, \"properties\":{\"name\":\"marko\",\"age\":32}, \"outE\":[{\"inId\":2, \"label\":\"knows\"}, {\"inId\":3, \"label\":\"created\"}]}");
        assertEquals(vertex.getId(), 1l);
        assertTrue(vertex.getOutEdges().iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getProperty("age"), 32l);
        List<Edge> edges = BaseTest.asList(vertex.getOutEdges());
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("knows") || edge.getLabel().equals("created"));
        }
        assertEquals(edges.size(), 2);
    }
}
