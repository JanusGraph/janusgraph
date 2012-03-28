package com.thinkaurelius.faunus.io.graph;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusEdgeTest extends TestCase {

    public void testSerialization1() throws IOException {

        FaunusEdge edge1 = new FaunusEdge(new FaunusVertex(1), new FaunusVertex(2), "knows");
        assertEquals(edge1.getLabel(), "knows");
        assertEquals(edge1.getOutVertex().getId(), 1l);
        assertEquals(edge1.getInVertex().getId(), 2l);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        edge1.write(out);

        FaunusEdge edge2 = new FaunusEdge(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(edge1, edge2);
        assertEquals(edge1.compareTo(edge2), 0);
        assertEquals(edge2.compareTo(edge1), 0);
        assertEquals(edge2.getId(), -1l);
        assertEquals(edge2.getLabel(), "knows");
        assertEquals(edge2.getOutVertex().getId(), 1l);
        assertEquals(edge2.getInVertex().getId(), 2l);

    }

    public void testSerialization2() throws IOException {

        FaunusEdge edge1 = new FaunusEdge(new FaunusVertex(1), new FaunusVertex(2), "knows");
        edge1.setProperty("weight", 0.5f);
        assertEquals(edge1.getLabel(), "knows");
        assertEquals(edge1.getOutVertex().getId(), 1l);
        assertEquals(edge1.getInVertex().getId(), 2l);
        assertEquals(edge1.getProperty("weight"), 0.5f);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        edge1.write(out);

        FaunusEdge edge2 = new FaunusEdge(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(edge1, edge2);
        assertEquals(edge1.compareTo(edge2), 0);
        assertEquals(edge2.compareTo(edge1), 0);
        assertEquals(edge2.getId(), -1l);
        assertEquals(edge2.getLabel(), "knows");
        assertEquals(edge2.getOutVertex().getId(), 1l);
        assertEquals(edge2.getInVertex().getId(), 2l);
        assertEquals(edge2.getProperty("weight"), 0.5f);
        assertEquals(edge2.getPropertyKeys().size(), 1);

    }
}
