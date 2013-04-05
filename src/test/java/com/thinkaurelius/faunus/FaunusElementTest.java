package com.thinkaurelius.faunus;

import junit.framework.TestCase;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusElementTest extends TestCase {

    public void testBasicSerialization() throws IOException {
        FaunusVertex vertex1 = new FaunusVertex(10);
        FaunusVertex vertex2 = new FaunusVertex(Long.MAX_VALUE);

        ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
        vertex1.write(new DataOutputStream(bytes1));
        assertEquals(bytes1.size(), 6);
        // 1 long id + 1 boolean path + 1 variable int paths + 1 short properties +  2 vinteger edge types (2)
        // ? + 1 + 1 + 2 + 2 + 2 = 11 bytes + 1 byte long id

        ByteArrayOutputStream bytes2 = new ByteArrayOutputStream();
        vertex2.write(new DataOutputStream(bytes2));
        assertEquals(bytes2.size(), 14);
        // 1 long id + 1 boolean path + 1 int paths + 1 short properties + 2 vinteger edge types (2)
        // ? + 1 + 1 + 2 + 2 + 2 = 11 bytes + 9 byte long id

        final Long id1 = WritableUtils.readVLong(new DataInputStream(new ByteArrayInputStream(bytes1.toByteArray())));
        final Long id2 = WritableUtils.readVLong(new DataInputStream(new ByteArrayInputStream(bytes2.toByteArray())));

        assertEquals(id1, new Long(10l));
        assertEquals(id2, new Long(Long.MAX_VALUE));
    }

    public void testElementComparator() throws IOException {
        FaunusVertex a = new FaunusVertex(10);
        FaunusVertex b = new FaunusVertex(Long.MAX_VALUE);
        FaunusVertex c = new FaunusVertex(10);
        FaunusVertex d = new FaunusVertex(12);

        assertEquals(a.compareTo(a), 0);
        assertEquals(a.compareTo(b), -1);
        assertEquals(a.compareTo(c), 0);
        assertEquals(a.compareTo(d), -1);

        assertEquals(b.compareTo(a), 1);
        assertEquals(b.compareTo(b), 0);
        assertEquals(b.compareTo(c), 1);
        assertEquals(b.compareTo(d), 1);

        assertEquals(c.compareTo(a), 0);
        assertEquals(c.compareTo(b), -1);
        assertEquals(c.compareTo(c), 0);
        assertEquals(c.compareTo(d), -1);

        assertEquals(d.compareTo(a), 1);
        assertEquals(d.compareTo(b), -1);
        assertEquals(d.compareTo(c), 1);
        assertEquals(d.compareTo(d), 0);

        ByteArrayOutputStream aBytes = new ByteArrayOutputStream();
        a.write(new DataOutputStream(aBytes));
        ByteArrayOutputStream bBytes = new ByteArrayOutputStream();
        b.write(new DataOutputStream(bBytes));
        ByteArrayOutputStream cBytes = new ByteArrayOutputStream();
        c.write(new DataOutputStream(cBytes));
        ByteArrayOutputStream dBytes = new ByteArrayOutputStream();
        d.write(new DataOutputStream(dBytes));

        //////// test raw byte comparator

        FaunusElement.Comparator comparator = new FaunusElement.Comparator();

        assertEquals(0, comparator.compare(aBytes.toByteArray(), 0, aBytes.size(), aBytes.toByteArray(), 0, aBytes.size()));
        assertEquals(-1, comparator.compare(aBytes.toByteArray(), 0, aBytes.size(), bBytes.toByteArray(), 0, bBytes.size()));
        assertEquals(0, comparator.compare(aBytes.toByteArray(), 0, aBytes.size(), cBytes.toByteArray(), 0, cBytes.size()));
        assertEquals(-1, comparator.compare(aBytes.toByteArray(), 0, aBytes.size(), dBytes.toByteArray(), 0, dBytes.size()));

        assertEquals(1, comparator.compare(bBytes.toByteArray(), 0, bBytes.size(), aBytes.toByteArray(), 0, aBytes.size()));
        assertEquals(0, comparator.compare(bBytes.toByteArray(), 0, bBytes.size(), bBytes.toByteArray(), 0, bBytes.size()));
        assertEquals(1, comparator.compare(bBytes.toByteArray(), 0, bBytes.size(), cBytes.toByteArray(), 0, cBytes.size()));
        assertEquals(1, comparator.compare(bBytes.toByteArray(), 0, bBytes.size(), dBytes.toByteArray(), 0, dBytes.size()));

        assertEquals(0, comparator.compare(cBytes.toByteArray(), 0, cBytes.size(), aBytes.toByteArray(), 0, aBytes.size()));
        assertEquals(-1, comparator.compare(cBytes.toByteArray(), 0, cBytes.size(), bBytes.toByteArray(), 0, bBytes.size()));
        assertEquals(0, comparator.compare(cBytes.toByteArray(), 0, cBytes.size(), cBytes.toByteArray(), 0, cBytes.size()));
        assertEquals(-1, comparator.compare(cBytes.toByteArray(), 0, cBytes.size(), dBytes.toByteArray(), 0, dBytes.size()));

        assertEquals(1, comparator.compare(dBytes.toByteArray(), 0, dBytes.size(), aBytes.toByteArray(), 0, aBytes.size()));
        assertEquals(-1, comparator.compare(dBytes.toByteArray(), 0, dBytes.size(), bBytes.toByteArray(), 0, bBytes.size()));
        assertEquals(1, comparator.compare(dBytes.toByteArray(), 0, dBytes.size(), cBytes.toByteArray(), 0, cBytes.size()));
        assertEquals(0, comparator.compare(dBytes.toByteArray(), 0, dBytes.size(), dBytes.toByteArray(), 0, dBytes.size()));
    }

    public void testSettingIdPropertyException() {
        FaunusVertex a = new FaunusVertex(10l);
        try {
            a.setProperty(Tokens.ID, 11l);
            assertFalse(true);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        FaunusEdge b = new FaunusEdge(1l, 2l, 13l, "self");
        try {
            b.setProperty(Tokens.ID, 10);
            assertFalse(true);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        try {
            b.setProperty(Tokens.ID, 10);
            assertFalse(true);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

    }

    public void testPathIteratorRemove() {
        FaunusVertex vertex1 = new FaunusVertex(10);
        assertEquals(vertex1.pathCount(), 0);
        vertex1.enablePath(true);
        assertEquals(vertex1.pathCount(), 0);
        vertex1.addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(2l)), false);
        vertex1.addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(3l)), false);
        vertex1.addPath((List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(4l)), false);
        assertEquals(vertex1.pathCount(), 3);
        Iterator<List<FaunusElement.MicroElement>> itty = vertex1.getPaths().iterator();
        while (itty.hasNext()) {
            if (itty.next().get(1).getId() == 3l)
                itty.remove();
        }
        assertEquals(vertex1.pathCount(), 2);
    }

    public void testPathHash() {
        List<FaunusElement.MicroElement> path1 = (List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(2l));
        List<FaunusElement.MicroElement> path2 = (List) Arrays.asList(new FaunusVertex.MicroVertex(1l), new FaunusVertex.MicroVertex(1l));

        assertEquals(new HashSet(path1).size(), 2);
        assertEquals(new HashSet(path2).size(), 1);
    }

}
