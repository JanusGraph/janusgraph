package com.thinkaurelius.faunus.io.graph.util;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderTest extends TestCase {

    public void testRawComparison() throws IOException {
        Holder<FaunusVertex> holder1 = new Holder<FaunusVertex>(new FaunusVertex(10));
        Holder<FaunusVertex> holder2 = new Holder<FaunusVertex>(new FaunusVertex(11));

        ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
        holder1.write(new DataOutputStream(bytes1));
        ByteArrayOutputStream bytes2 = new ByteArrayOutputStream();
        holder2.write(new DataOutputStream(bytes2));

        assertEquals(-1, holder1.compare(bytes1.toByteArray(), 0, bytes1.size(), bytes2.toByteArray(), 0, bytes2.size()));
        assertEquals(1, holder1.compare(bytes2.toByteArray(), 0, bytes2.size(), bytes1.toByteArray(), 0, bytes1.size()));
        assertEquals(0, holder1.compare(bytes1.toByteArray(), 0, bytes1.size(), bytes1.toByteArray(), 0, bytes1.size()));
    }

    public void testSerialization1() throws IOException {
        FaunusVertex vertex = new FaunusVertex(1l);
        Holder<FaunusVertex> holder1 = new Holder<FaunusVertex>(vertex);
        assertEquals(holder1.get(), vertex);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        holder1.write(new DataOutputStream(bytes));
        Holder<FaunusVertex> holder2 = new Holder<FaunusVertex>(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(holder1, holder2);
        assertEquals(holder2.get(), vertex);

        assertEquals(holder1.compareTo(holder2), 0);
    }
}
