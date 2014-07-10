package com.thinkaurelius.titan.hadoop.mapreduce.util;

import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Tokens;

import junit.framework.TestCase;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementPickerTest extends TestCase {

    public void testPathCount() {
        HadoopVertex vertex = new HadoopVertex(EmptyConfiguration.immutable(), 1l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._COUNT), 0l);
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens._COUNT), "0");
        vertex.incrPath(199);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._COUNT), 199l);
        vertex.incrPath(1);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._COUNT), 200l);
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens._COUNT), "200");
    }

    public void testId() {
        HadoopVertex vertex = new HadoopVertex(EmptyConfiguration.immutable(), 10l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._ID), 10l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens.ID), 10l);
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens._ID), "10");
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens.ID), "10");
    }

    public void testLabel() {
        HadoopVertex vertex = new HadoopVertex(EmptyConfiguration.immutable(), 10l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens.LABEL), null);
        vertex.setProperty(Tokens.LABEL, "aType");
        assertEquals(ElementPicker.getProperty(vertex, Tokens.LABEL), "aType");

        StandardFaunusEdge edge = new StandardFaunusEdge(EmptyConfiguration.immutable(), 1l, 10l, 10l, "knows");
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

    public void testMultiProperties() {
        HadoopVertex vertex = new HadoopVertex(EmptyConfiguration.immutable(), 10l);
        vertex.addProperty("name","marko1");
        vertex.addProperty("name","marko2");
        assertEquals(vertex.getPropertyKeys().size(), 1);
        assertTrue(((Collection) ElementPicker.getProperty(vertex, "name")).contains("marko1"));
        assertTrue(((Collection) ElementPicker.getProperty(vertex, "name")).contains("marko2"));
    }
}
