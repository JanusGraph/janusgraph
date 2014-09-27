package com.thinkaurelius.titan.hadoop.mapreduce.util;

import com.thinkaurelius.titan.hadoop.*;

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import junit.framework.TestCase;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementPickerTest extends TestCase {

    private FaunusSchemaManager typeManager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeManager = FaunusSchemaManager.getTypeManager(new ModifiableHadoopConfiguration());
        typeManager.setSchemaProvider(TestSchemaProvider.MULTIPLICITY);
        typeManager.clear();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        typeManager.setSchemaProvider(DefaultSchemaProvider.INSTANCE);
        typeManager.clear();
    }

    public void testPathCount() {
        FaunusVertex vertex = new FaunusVertex(new ModifiableHadoopConfiguration(), 1l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._COUNT), 0l);
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens._COUNT), "0");
        vertex.incrPath(199);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._COUNT), 199l);
        vertex.incrPath(1);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._COUNT), 200l);
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens._COUNT), "200");
    }

    public void testId() {
        FaunusVertex vertex = new FaunusVertex(new ModifiableHadoopConfiguration(), 10l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens._ID), 10l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens.ID), 10l);
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens._ID), "10");
        assertEquals(ElementPicker.getPropertyAsString(vertex, Tokens.ID), "10");
    }

    public void testLabel() {
        FaunusVertex vertex = new FaunusVertex(new ModifiableHadoopConfiguration(), 10l);
        assertEquals(ElementPicker.getProperty(vertex, Tokens.LABEL), "_default");
        try {
            vertex.setProperty(Tokens.LABEL, "aType");
            fail();
        } catch (IllegalArgumentException e) {}
        vertex.setVertexLabel("aType");
        assertEquals(ElementPicker.getProperty(vertex, Tokens.LABEL), "aType");

        StandardFaunusEdge edge = new StandardFaunusEdge(new ModifiableHadoopConfiguration(), 1l, 10l, 10l, "knows");
        assertEquals(ElementPicker.getProperty(edge, Tokens.LABEL), "knows");
        try {
            edge.setProperty(Tokens.LABEL, "self");
            fail();
        } catch (IllegalArgumentException e) { }
        assertEquals(ElementPicker.getProperty(edge, Tokens.LABEL), "knows");
        assertEquals(ElementPicker.getPropertyAsString(edge, Tokens.LABEL), "knows");

    }

    public void testMultiProperties() {
        FaunusVertex vertex = new FaunusVertex(new ModifiableHadoopConfiguration(), 10l);
        vertex.addProperty("namelist","marko1");
        vertex.addProperty("namelist","marko2");
        assertEquals(vertex.getPropertyKeys().size(), 1);
        assertTrue(((Collection) ElementPicker.getProperty(vertex, "namelist")).contains("marko1"));
        assertTrue(((Collection) ElementPicker.getProperty(vertex, "namelist")).contains("marko2"));
    }
}
