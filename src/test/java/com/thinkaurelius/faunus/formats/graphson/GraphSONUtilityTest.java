package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.BaseTest;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.graphson.GraphSONUtility;
import com.tinkerpop.blueprints.Edge;
import junit.framework.TestCase;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.List;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONUtilityTest extends TestCase {

    public void testParser1() throws IOException {
        FaunusVertex vertex = GraphSONUtility.fromJSON("{\"_id\":1}");
        assertEquals(vertex.getId(), 1l);
        assertFalse(vertex.getEdges(OUT).iterator().hasNext());
    }

    public void testParser2() throws IOException {
        FaunusVertex vertex = GraphSONUtility.fromJSON("{\"_id\":1, \"name\":\"marko\",\"age\":32}");
        assertEquals(vertex.getId(), 1l);
        assertFalse(vertex.getEdges(OUT).iterator().hasNext());
        assertFalse(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getProperty("age"), 32);
    }

    public void testParser3() throws IOException {
        FaunusVertex vertex = GraphSONUtility.fromJSON("{\"_id\":1, \"name\":\"marko\",\"age\":32, \"_outE\":[{\"_inV\":2, \"_label\":\"knows\"}, {\"_inV\":3, \"_label\":\"created\"}]}");
        assertEquals(vertex.getId(), 1l);
        assertTrue(vertex.getEdges(OUT).iterator().hasNext());
        assertFalse(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "marko");
        assertEquals(vertex.getProperty("age"), 32);
        List<Edge> edges = BaseTest.asList(vertex.getEdges(OUT));
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("knows") || edge.getLabel().equals("created"));
        }
        assertEquals(edges.size(), 2);
    }

    public void testParser4() throws IOException {
        FaunusVertex vertex = GraphSONUtility.fromJSON("{\"_id\":4, \"name\":\"josh\", \"age\":32, \"_outE\":[{\"_inV\":3, \"_label\":\"created\", \"weight\":0.4}, {\"_inV\":5, \"_label\":\"created\", \"weight\":1.0}], \"_inE\":[{\"_outV\":1, \"_label\":\"knows\", \"weight\":1.0}]}");
        assertEquals(vertex.getId(), 4l);
        assertTrue(vertex.getEdges(OUT).iterator().hasNext());
        assertTrue(vertex.getEdges(IN).iterator().hasNext());
        assertEquals(vertex.getPropertyKeys().size(), 2);
        assertEquals(vertex.getProperty("name"), "josh");
        assertEquals(vertex.getProperty("age"), 32);
        List<Edge> edges = BaseTest.asList(vertex.getEdges(OUT));
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("created"));
        }
        assertEquals(edges.size(), 2);
        edges = BaseTest.asList(vertex.getEdges(IN));
        for (final Edge edge : edges) {
            assertTrue(edge.getLabel().equals("knows"));
            assertEquals(edge.getProperty("weight"), 1);
        }
        assertEquals(edges.size(), 1);
    }

    public void testWriter1() throws Exception {
        FaunusVertex marko = new FaunusVertex(1l);
        marko.setProperty("name", "marko");
        FaunusVertex stephen = new FaunusVertex(2l);
        stephen.setProperty("name", "stephen");
        marko.addEdge(OUT, new FaunusEdge(marko, stephen, "knows")).setProperty("weight", 1);

        JSONObject m = GraphSONUtility.toJSON(marko);
        JSONObject s = GraphSONUtility.toJSON(stephen);

        assertEquals(m.getString("name"), "marko");
        assertEquals(m.getLong("_id"), 1l);
        assertEquals(m.getJSONArray("_outE").length(), 1);
        assertEquals(m.getJSONArray("_outE").getJSONObject(0).getLong("weight"), 1);
        assertNull(m.optJSONArray("_inE"));

        assertEquals(s.getString("name"), "stephen");
        assertEquals(s.getLong("_id"), 2l);
        assertNull(s.optJSONArray("_outE"));
        assertNull(s.optJSONArray("_inE"));

    }
}
