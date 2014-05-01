package com.thinkaurelius.titan.hadoop.formats.rexster.util;

import com.thinkaurelius.titan.hadoop.BaseTest;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexToFaunusBinaryTest extends BaseTest {
    public void testTrue() {
    }

    /*public void testConversion() throws IOException {

        Graph graph = new TinkerGraph();
        Vertex marko = graph.addVertex(1);
        marko.setProperty("name", "marko");
        marko.setProperty("age", 32);
        Vertex stephen = graph.addVertex(3);
        stephen.setProperty("name", "stephen");
        stephen.setProperty("weight", 160.42);
        stephen.setProperty("male", true);
        Edge e = graph.addEdge(null, marko, stephen, "knows");
        e.setProperty("weight", 0.2);
        e.setProperty("type", "coworker");

        ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(bytes1);
        VertexToFaunusBinary.write(marko, out);
        VertexToFaunusBinary.write(stephen, out);

        DataInput in = new DataInputStream(new ByteArrayInputStream(bytes1.toByteArray()));

        FaunusVertex markoFaunus = new FaunusVertex(new EmptyConfiguration(), in);
        assertEquals(markoFaunus.getProperty("name"), "marko");
        assertEquals(markoFaunus.getProperty("age"), 32);
        assertEquals(markoFaunus.getPropertyKeys().size(), 2);
        assertEquals(asList(markoFaunus.getEdges(Direction.OUT)).size(), 1);
        assertFalse(markoFaunus.getEdges(Direction.IN).iterator().hasNext());
        assertTrue(markoFaunus.getEdges(Direction.OUT, "knows").iterator().hasNext());
        assertFalse(markoFaunus.getEdges(Direction.OUT, "blah").iterator().hasNext());
        FaunusEdge edge = (FaunusEdge) markoFaunus.getEdges(Direction.OUT).iterator().next();
        assertEquals(edge.getLabel(), "knows");
        assertEquals(edge.getProperty("weight"), 0.2);
        assertEquals(edge.getProperty("type"), "coworker");
        assertEquals(edge.getPropertyKeys().size(), 2);
        assertEquals(edge.getVertex(Direction.IN).getId(), 3l);
        assertEquals(edge.getVertex(Direction.OUT).getId(), 1l);

        FaunusVertex stephenFaunus = new FaunusVertex(new EmptyConfiguration(), in);
        assertEquals(stephenFaunus.getProperty("name"), "stephen");
        assertEquals(stephenFaunus.getProperty("weight"), 160.42);
        assertTrue((Boolean) stephenFaunus.getProperty("male"));
        assertEquals(stephenFaunus.getPropertyKeys().size(), 3);
        assertEquals(asList(stephenFaunus.getEdges(Direction.IN)).size(), 1);
        assertFalse(stephenFaunus.getEdges(Direction.OUT).iterator().hasNext());
        assertTrue(stephenFaunus.getEdges(Direction.IN, "knows").iterator().hasNext());
        assertFalse(stephenFaunus.getEdges(Direction.IN, "blah").iterator().hasNext());
        edge = (FaunusEdge) stephenFaunus.getEdges(Direction.IN).iterator().next();
        assertEquals(edge.getLabel(), "knows");
        assertEquals(edge.getProperty("weight"), 0.2);
        assertEquals(edge.getProperty("type"), "coworker");
        assertEquals(edge.getPropertyKeys().size(), 2);
        assertEquals(edge.getVertex(Direction.IN).getId(), 3l);
        assertEquals(edge.getVertex(Direction.OUT).getId(), 1l);

    } */
}
