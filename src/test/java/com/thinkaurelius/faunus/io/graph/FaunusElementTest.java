package com.thinkaurelius.faunus.io.graph;

import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusElementTest extends TestCase {

    public void testByteComparisonAlgorithm() throws IOException {
        FaunusVertex vertex1 = new FaunusVertex(10);
        FaunusVertex vertex2 = new FaunusVertex(11);

        ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
        vertex1.write(new DataOutputStream(bytes1));
        ByteArrayOutputStream bytes2 = new ByteArrayOutputStream();
        vertex2.write(new DataOutputStream(bytes2));

        assertEquals(bytes1.toByteArray()[0], FaunusElement.ElementType.VERTEX.val);

        final Long id1 = ByteBuffer.wrap(bytes1.toByteArray(), 1, 9).getLong();
        final Long id2 = ByteBuffer.wrap(bytes2.toByteArray(), 1, 9).getLong();

        assertEquals(id1, new Long(10l));
        assertEquals(id2, new Long(11l));
    }
}
