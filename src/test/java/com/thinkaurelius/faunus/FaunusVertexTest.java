package com.thinkaurelius.faunus;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertexTest extends BaseTest {

    public void testRawComparison() throws IOException {
        FaunusVertex vertex1 = new FaunusVertex(10);
        FaunusVertex vertex2 = new FaunusVertex(11);

        ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
        vertex1.write(new DataOutputStream(bytes1));
        ByteArrayOutputStream bytes2 = new ByteArrayOutputStream();
        vertex2.write(new DataOutputStream(bytes2));

        assertEquals(-1, new FaunusVertex.Comparator().compare(bytes1.toByteArray(), 0, bytes1.size(), bytes2.toByteArray(), 0, bytes2.size()));
        assertEquals(1, new FaunusVertex.Comparator().compare(bytes2.toByteArray(), 0, bytes2.size(), bytes1.toByteArray(), 0, bytes1.size()));
        assertEquals(0, new FaunusVertex.Comparator().compare(bytes1.toByteArray(), 0, bytes1.size(), bytes1.toByteArray(), 0, bytes1.size()));
    }

    public void testSerialization1() throws IOException {

        FaunusVertex vertex1 = new FaunusVertex(10);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        vertex1.write(out);

        FaunusVertex vertex2 = new FaunusVertex(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.compareTo(vertex2), 0);
        assertEquals(vertex2.compareTo(vertex1), 0);
        assertEquals(vertex2.getId(), 10l);
        assertFalse(vertex2.getEdges(Direction.OUT).iterator().hasNext());
        assertFalse(vertex2.getEdges(Direction.IN).iterator().hasNext());

    }

    public void testSerialization2() throws IOException {

        FaunusVertex vertex1 = new FaunusVertex(10);
        vertex1.addEdge(OUT, new FaunusEdge(vertex1, new FaunusVertex(2), "knows"));
        vertex1.addEdge(OUT, new FaunusEdge(vertex1, new FaunusVertex(3), "knows"));
        vertex1.setProperty("name", "marko");
        vertex1.setProperty("age", 32);
        vertex1.setProperty("longitude", 10.01d);
        vertex1.setProperty("latitude", 11.4f);
        vertex1.setProperty("size", 10l);
        assertEquals(vertex1.getPropertyKeys().size(), 5);
        assertEquals(vertex1.getProperty("name"), "marko");
        assertEquals(vertex1.getProperty("age"), 32);
        assertEquals(vertex1.getProperty("longitude"), 10.01d);
        assertEquals(vertex1.getProperty("latitude"), 11.4f);
        assertEquals(vertex1.getProperty("size"), 10l);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        vertex1.write(new DataOutputStream(bytes));
        FaunusVertex vertex2 = new FaunusVertex(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.compareTo(vertex2), 0);
        assertEquals(vertex2.compareTo(vertex1), 0);
        assertEquals(vertex2.getId(), 10l);
        assertEquals(vertex2.getPropertyKeys().size(), 5);
        assertEquals(vertex2.getProperty("name"), "marko");
        assertEquals(vertex2.getProperty("age"), 32);
        assertEquals(vertex2.getProperty("longitude"), 10.01d);
        assertEquals(vertex2.getProperty("latitude"), 11.4f);
        assertEquals(vertex1.getProperty("size"), 10l);

        Iterator<Edge> edges = vertex2.getEdges(Direction.OUT).iterator();
        assertTrue(edges.hasNext());
        assertEquals(edges.next().getLabel(), "knows");
        assertTrue(edges.hasNext());
        assertEquals(edges.next().getLabel(), "knows");
        assertFalse(edges.hasNext());

    }

    public void testSerialization3() throws IOException {

        FaunusVertex vertex1 = new FaunusVertex(10);
        vertex1.addEdge(OUT, new FaunusEdge(vertex1, new FaunusVertex(2), "knows"));
        vertex1.addEdge(IN, new FaunusEdge(new FaunusVertex(3), vertex1, "knows"));
        vertex1.setProperty("name", "marko");
        vertex1.setProperty("age", 32);
        vertex1.setProperty("longitude", 10.01d);
        vertex1.setProperty("latitude", 11.4f);
        vertex1.setProperty("size", 10l);
        assertEquals(vertex1.getPropertyKeys().size(), 5);
        assertEquals(vertex1.getProperty("name"), "marko");
        assertEquals(vertex1.getProperty("age"), 32);
        assertEquals(vertex1.getProperty("longitude"), 10.01d);
        assertEquals(vertex1.getProperty("latitude"), 11.4f);
        assertEquals(vertex1.getProperty("size"), 10l);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        vertex1.write(new DataOutputStream(bytes));
        FaunusVertex vertex2 = new FaunusVertex(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.compareTo(vertex2), 0);
        assertEquals(vertex2.compareTo(vertex1), 0);
        assertEquals(vertex2.getId(), 10l);
        assertEquals(vertex2.getPropertyKeys().size(), 5);
        assertEquals(vertex2.getProperty("name"), "marko");
        assertEquals(vertex2.getProperty("age"), 32);
        assertEquals(vertex2.getProperty("longitude"), 10.01d);
        assertEquals(vertex2.getProperty("latitude"), 11.4f);
        assertEquals(vertex1.getProperty("size"), 10l);

        Iterator<Edge> edges = vertex2.getEdges(Direction.OUT).iterator();
        assertTrue(edges.hasNext());
        Edge edge = edges.next();
        assertEquals(edge.getLabel(), "knows");
        assertEquals(edge.getVertex(Direction.IN).getId(), 2l);
        assertEquals(edge.getVertex(Direction.OUT).getId(), 10l);
        assertFalse(edges.hasNext());

        edges = vertex2.getEdges(Direction.IN).iterator();
        assertTrue(edges.hasNext());
        edge = edges.next();
        assertEquals(edge.getLabel(), "knows");
        assertEquals(edge.getVertex(Direction.IN).getId(), 10l);
        assertEquals(edge.getVertex(Direction.OUT).getId(), 3l);
        assertEquals(edge.getLabel(), "knows");
        assertFalse(edges.hasNext());

    }

    public void testNoProperties() throws IOException {
        FaunusVertex vertex1 = new FaunusVertex(1l);
        vertex1.addEdge(OUT, new FaunusEdge(vertex1, vertex1, "knows"));

        assertNull(vertex1.getProperty("name"));
        assertNull(vertex1.removeProperty("name"));
        assertEquals(vertex1.getPropertyKeys().size(), 0);
        assertEquals(vertex1.getProperties().size(), 0);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        vertex1.write(new DataOutputStream(bytes));
        FaunusVertex vertex2 = new FaunusVertex(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(vertex1, vertex2);
        assertEquals(vertex1.getId(), 1l);
        assertNull(vertex1.getProperty("name"));
        assertNull(vertex1.removeProperty("name"));
        assertEquals(vertex1.getPropertyKeys().size(), 0);
        assertEquals(vertex1.getProperties().size(), 0);
        assertEquals(asList(vertex1.getEdges(OUT)).size(), 1);
        assertEquals(asList(vertex1.getEdges(IN)).size(), 0);
        assertEquals(asList(vertex1.getEdges(BOTH)).size(), 1);

        assertEquals(vertex2, vertex1);
        assertEquals(vertex2.getId(), 1l);
        assertNull(vertex2.getProperty("age"));
        assertNull(vertex2.removeProperty("age"));
        assertEquals(vertex2.getPropertyKeys().size(), 0);
        assertEquals(vertex2.getProperties().size(), 0);
        assertEquals(asList(vertex2.getEdges(OUT)).size(), 1);
        assertEquals(asList(vertex2.getEdges(IN)).size(), 0);
        assertEquals(asList(vertex2.getEdges(BOTH)).size(), 1);
    }


}
