package com.thinkaurelius.faunus.mapreduce.util;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import junit.framework.TestCase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementPickerTest extends TestCase {

    public void testPathCount() {
        FaunusVertex vertex = new FaunusVertex(1l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._COUNT), 0l);
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens._COUNT), "0");
        vertex.incrPath(199);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._COUNT), 199l);
        vertex.incrPath(1);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._COUNT), 200l);
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens._COUNT), "200");
    }

    public void testId() {
        FaunusVertex vertex = new FaunusVertex(10l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._ID), 10l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens.ID), 10l);
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens._ID), "10");
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens.ID), "10");
    }

    public void testLabel() {
        FaunusVertex vertex = new FaunusVertex(10l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens.LABEL), null);
        vertex.setProperty(Tokens.LABEL, "aType");
        assertEquals(ElementPicker.getProperty(vertex, Tokens.LABEL), "aType");

        FaunusEdge edge = new FaunusEdge(1l, 10l, 10l, "knows");
        assertEquals(ElementPicker.getProperty(edge, Tokens.LABEL), "knows");
        try {
            edge.setProperty(Tokens.LABEL, "self");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        assertEquals(ElementPicker.getProperty(edge, Tokens.LABEL), "knows");
        assertEquals(ElementPicker.getPropertyAsString(edge, Tokens.LABEL), "knows");

    }
}
