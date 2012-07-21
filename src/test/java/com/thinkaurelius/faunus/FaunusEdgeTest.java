package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
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

    public void testSerialization1() throws IOException {

        FaunusEdge edge1 = new FaunusEdge(new FaunusVertex(1), new FaunusVertex(2), "knows");
        assertEquals(edge1.getLabel(), "knows");
        assertEquals(edge1.getVertex(Direction.OUT).getId(), 1l);
        assertEquals(edge1.getVertex(Direction.IN).getId(), 2l);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        edge1.write(out);

        FaunusEdge edge2 = new FaunusEdge(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(edge1, edge2);
        assertEquals(edge2.getId(), -1l);
        assertEquals(edge2.getLabel(), "knows");
        assertEquals(edge2.getVertex(Direction.OUT).getId(), 1l);
        assertEquals(edge2.getVertex(Direction.IN).getId(), 2l);

    }

    public void testSerialization2() throws IOException {

        FaunusEdge edge1 = new FaunusEdge(new FaunusVertex(1), new FaunusVertex(2), "knows");
        edge1.setProperty("weight", 0.5f);
        assertEquals(edge1.getLabel(), "knows");
        assertEquals(edge1.getVertex(Direction.OUT).getId(), 1l);
        assertEquals(edge1.getVertex(Direction.IN).getId(), 2l);
        assertEquals(edge1.getProperty("weight"), 0.5f);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        edge1.write(out);

        FaunusEdge edge2 = new FaunusEdge(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(edge1, edge2);
        assertEquals(edge2.getId(), -1l);
        assertEquals(edge2.getLabel(), "knows");
        assertEquals(edge2.getVertex(Direction.OUT).getId(), 1l);
        assertEquals(edge2.getVertex(Direction.IN).getId(), 2l);
        assertEquals(edge2.getProperty("weight"), 0.5f);
        assertEquals(edge2.getPropertyKeys().size(), 1);

    }
}
