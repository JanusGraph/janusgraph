package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.util.MicroElement;
import com.thinkaurelius.faunus.util.MicroVertex;
import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

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

        final Long id1 = ByteBuffer.wrap(bytes1.toByteArray(), 0, 8).getLong();
        final Long id2 = ByteBuffer.wrap(bytes2.toByteArray(), 0, 8).getLong();

        assertEquals(id1, new Long(10l));
        assertEquals(id2, new Long(11l));
    }

    public void testPathIteratorRemove() {
        FaunusVertex vertex1 = new FaunusVertex(10);
        assertEquals(vertex1.pathCount(), 0);
        vertex1.enablePath(true);
        assertEquals(vertex1.pathCount(), 0);
        vertex1.addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(2l)), false);
        vertex1.addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(3l)), false);
        vertex1.addPath((List) Arrays.asList(new MicroVertex(1l), new MicroVertex(4l)), false);
        assertEquals(vertex1.pathCount(), 3);
        Iterator<List<MicroElement>> itty = vertex1.getPaths().iterator();
        while (itty.hasNext()) {
            if (itty.next().get(1).getId() == 3l)
                itty.remove();
        }
        assertEquals(vertex1.pathCount(), 2);
    }

    public void testPathHash() {
        List<MicroElement> path1 = (List) Arrays.asList(new MicroVertex(1l), new MicroVertex(2l));
        List<MicroElement> path2 = (List) Arrays.asList(new MicroVertex(1l), new MicroVertex(1l));

        assertEquals(new HashSet(path1).size(), 2);
        assertEquals(new HashSet(path2).size(), 1);
    }

}
