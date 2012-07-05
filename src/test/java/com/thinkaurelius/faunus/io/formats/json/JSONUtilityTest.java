package com.thinkaurelius.faunus.io.formats.json;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.Edge;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JSONUtilityTest extends TestCase {

    public void testParser1() throws IOException {
        JSONUtility reader = new JSONUtility();
        FaunusVertex vertex = reader.fromJSON("{\"id\":1}");
        assertEquals(vertex.getId(), 1l);
        assertFalse(vertex.getEdges(OUT).iterator().hasNext());

    }

    public void testParser2() throws IOException {
        JSONUtility reader = new JSONUtility();
        FaunusVertex vertex = reader.fromJSON("{\"id\":1, \"properties\":{\"name\":\"marko\",\"age\":32}}");
        assertEquals(vertex.getId(), 1l);
        assertFalse(vertex.getEdges(OUT).iterator().hasNext());
        assertFalse(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getProperty("age"), 32l);
    }

    public void testParser3() throws IOException {
        JSONUtility reader = new JSONUtility();
        FaunusVertex vertex = reader.fromJSON("{\"id\":1, \"properties\":{\"name\":\"marko\",\"age\":32}, \"outE\":[{\"inId\":2, \"label\":\"knows\"}, {\"inId\":3, \"label\":\"created\"}]}");
        assertEquals(vertex.getId(), 1l);
        assertTrue(vertex.getEdges(OUT).iterator().hasNext());
        assertFalse(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getProperty("age"), 32l);
        List<Edge> edges = BaseTest.asList(vertex.getEdges(OUT));
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("knows") || edge.getLabel().equals("created"));
        }
        assertEquals(edges.size(), 2);
    }

    public void testParser4() throws IOException {
        JSONUtility reader = new JSONUtility();
        FaunusVertex vertex = reader.fromJSON("{\"id\":4, \"properties\":{\"name\":\"josh\", \"age\":32}, \"outE\":[{\"inId\":3, \"label\":\"created\", \"properties\": {\"weight\":0.4}}, {\"inId\":5, \"label\":\"created\", \"properties\":{\"weight\":1.0}}], \"inE\":[{\"outId\":1, \"label\":\"knows\", \"properties\":{\"weight\":1.0}}]}");
        assertEquals(vertex.getId(), 4l);
        assertTrue(vertex.getEdges(OUT).iterator().hasNext());
        assertTrue(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "josh");
        assertEquals(vertex.getProperty("age"), 32l);
        List<Edge> edges = BaseTest.asList(vertex.getEdges(OUT));
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("created"));
        }
        assertEquals(edges.size(), 2);
        edges = BaseTest.asList(vertex.getEdges(IN));
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("knows"));
            assertEquals(edge.getProperty("weight"), 1.0);
        }
        assertEquals(edges.size(), 1);
    }
}
