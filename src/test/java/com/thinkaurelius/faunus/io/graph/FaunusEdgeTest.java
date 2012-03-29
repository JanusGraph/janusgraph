package com.thinkaurelius.faunus.io.graph;

import junit.framework.TestCase;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusEdgeTest extends TestCase {

    public void testRawComparison() throws IOException {
        FaunusEdge edge1 = new FaunusEdge(new FaunusVertex(1), new FaunusVertex(2), "knows");
        FaunusEdge edge2 = new FaunusEdge(new FaunusVertex(1), new FaunusVertex(3), "knows");
        FaunusEdge edge3 = new FaunusEdge(new FaunusVertex(2), new FaunusVertex(2), "created");
        FaunusEdge edge4 = new FaunusEdge(new FaunusVertex(2), new FaunusVertex(2), "knows");

        ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
        edge1.write(new DataOutputStream(bytes1));
        ByteArrayOutputStream bytes2 = new ByteArrayOutputStream();
        edge2.write(new DataOutputStream(bytes2));
        ByteArrayOutputStream bytes3 = new ByteArrayOutputStream();
        edge3.write(new DataOutputStream(bytes3));
        ByteArrayOutputStream bytes4 = new ByteArrayOutputStream();
        edge4.write(new DataOutputStream(bytes4));

        RawComparator comparator = WritableComparator.get(FaunusEdge.class);

        assertEquals(-1, comparator.compare(bytes1.toByteArray(), 0, bytes1.size(), bytes2.toByteArray(), 0, bytes2.size()));
        assertEquals(1, comparator.compare(bytes2.toByteArray(), 0, bytes2.size(), bytes1.toByteArray(), 0, bytes1.size()));
        assertEquals(0, comparator.compare(bytes1.toByteArray(), 0, bytes1.size(), bytes1.toByteArray(), 0, bytes1.size()));

        assertEquals(-1, comparator.compare(bytes1.toByteArray(), 0, bytes1.size(), bytes3.toByteArray(), 0, bytes3.size()));
        assertEquals(0, comparator.compare(bytes3.toByteArray(), 0, bytes3.size(), bytes4.toByteArray(), 0, bytes4.size()));
    }

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
