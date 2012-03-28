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
public class TaggedHolderTest extends TestCase {

    public void testSerialization1() throws IOException {
        FaunusVertex vertex = new FaunusVertex(1l);
        TaggedHolder<FaunusVertex> holder1 = new TaggedHolder<FaunusVertex>('a', vertex);
        assertEquals(holder1.get(), vertex);
        assertEquals(holder1.getTag(), 'a');

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        holder1.write(new DataOutputStream(bytes));
        TaggedHolder<FaunusVertex> holder2 = new TaggedHolder<FaunusVertex>(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(holder1, holder2);
        assertEquals(holder2.get(), vertex);
        assertEquals(holder2.getTag(), 'a');

        assertEquals(holder1.compareTo(holder2), 0);
    }
}
