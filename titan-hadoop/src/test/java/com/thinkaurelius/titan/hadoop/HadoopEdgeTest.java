package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Direction;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopEdgeTest extends TestCase {

    public void testSimpleSerialization() throws IOException {

        HadoopEdge edge1 = new HadoopEdge(EmptyConfiguration.immutable(), 1, 2, "knows");
        assertEquals(edge1.getLabel(), "knows");
        assertEquals(edge1.getVertex(Direction.OUT).getId(), 1l);
        assertEquals(edge1.getVertex(Direction.IN).getId(), 2l);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        edge1.write(out);
        assertEquals(bytes.size(), 12);
        // long id (vlong), path counters (vlong), long vid (vlong), long vid (vlong), String label
        // 1 + 1 + 1 + 1 + 10 byte label = 13


        HadoopEdge edge2 = new HadoopEdge(EmptyConfiguration.immutable(), new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        assertEquals(edge1, edge2);
        assertEquals(edge2.getId(), -1l);
        assertEquals(edge2.getLabel(), "knows");
        assertEquals(edge2.getVertex(Direction.OUT).getId(), 1l);
        assertEquals(edge2.getVertex(Direction.IN).getId(), 2l);

    }

    public void testSerializationWithProperties() throws IOException {

        HadoopEdge edge1 = new HadoopEdge(EmptyConfiguration.immutable(), 1, 2, "knows");
        edge1.setProperty("weight", 0.5f);
        edge1.setProperty("type", "coworker");
        edge1.setProperty("alive", true);
        edge1.setProperty("bigLong", Long.MAX_VALUE);
        edge1.setProperty("age", 1);
        assertEquals(edge1.getLabel(), "knows");
        assertEquals(edge1.getVertex(Direction.OUT).getId(), 1l);
        assertEquals(edge1.getVertex(Direction.IN).getId(), 2l);
        assertEquals(edge1.getProperty("weight"), 0.5f);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        edge1.write(out);

        HadoopEdge edge2 = new HadoopEdge(new EmptyConfiguration(), new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(edge1, edge2);
        assertEquals(edge2.getId(), -1l);
        assertEquals(edge2.getLabel(), "knows");
        assertEquals(edge2.getVertex(Direction.OUT).getId(), 1l);
        assertEquals(edge2.getVertex(Direction.IN).getId(), 2l);
        assertEquals(edge2.getProperty("weight"), 0.5f);
        assertEquals(edge2.getProperty("type"), "coworker");
        assertEquals(edge2.getProperty("alive"), true);
        assertEquals(edge2.getProperty("bigLong"), Long.MAX_VALUE);
        assertEquals(edge2.getProperty("age"), 1);
        assertEquals(edge2.getPropertyKeys().size(), 5);

    }
}
