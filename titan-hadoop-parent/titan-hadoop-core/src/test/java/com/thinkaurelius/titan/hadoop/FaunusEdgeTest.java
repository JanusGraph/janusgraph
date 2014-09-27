package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
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
public class FaunusEdgeTest extends TestCase {

    private FaunusSchemaManager typeManager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeManager = FaunusSchemaManager.getTypeManager(new ModifiableHadoopConfiguration());
        typeManager.setSchemaProvider(TestSchemaProvider.MULTIPLICITY_ID);
        typeManager.clear();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        typeManager.setSchemaProvider(DefaultSchemaProvider.INSTANCE);
        typeManager.clear();
    }

    public void testSimpleSerialization() throws IOException {

        StandardFaunusEdge edge1 = new StandardFaunusEdge(new ModifiableHadoopConfiguration(), 1, 2, "knows");
        assertEquals(edge1.getLabel(), "knows");
        assertEquals(edge1.getVertex(Direction.OUT).getLongId(), 1l);
        assertEquals(edge1.getVertex(Direction.IN).getLongId(), 2l);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        edge1.write(out);
        assertEquals(13, bytes.size());
        // long id (vlong), path counters (vlong), long vid (vlong), long vid (vlong), String label
        // 1 + 1 + 1 + 1 + 10 byte label = 13


        StandardFaunusEdge edge2 = new StandardFaunusEdge(new ModifiableHadoopConfiguration(), new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        assertEquals(edge1, edge2);
        assertNull(edge2.getId());
        assertEquals(edge2.getLongId(), -1l);
        assertEquals(edge2.getLabel(), "knows");
        assertEquals(edge2.getVertex(Direction.OUT).getLongId(), 1l);
        assertEquals(edge2.getVertex(Direction.IN).getLongId(), 2l);

    }

    public void testRelationIdentifier() {
        StandardFaunusEdge edge1 = new StandardFaunusEdge(new ModifiableHadoopConfiguration(), 1, 11, 12, "knows");
        RelationIdentifier eid = (RelationIdentifier) edge1.getId();
        assertNotNull(eid);
        long[] eidl = eid.getLongRepresentation();
        assertEquals(4,eidl.length);
        assertEquals(1,eidl[0]);
        assertEquals(11,eidl[1]);
        assertEquals(typeManager.getRelationType("knows").getLongId(),eidl[2]);
        assertEquals(12,eidl[3]);

    }

    public void testSerializationWithProperties() throws IOException {

        StandardFaunusEdge edge1 = new StandardFaunusEdge(new ModifiableHadoopConfiguration(), 1, 2, "knows");
        edge1.setProperty("weight", 0.5f);
        edge1.setProperty("type", "coworker");
        edge1.setProperty("alive", true);
        edge1.setProperty("bigLong", Long.MAX_VALUE);
        edge1.setProperty("age", 1);
        assertEquals(edge1.getLabel(), "knows");
        assertEquals(edge1.getVertex(Direction.OUT).getLongId(), 1l);
        assertEquals(edge1.getVertex(Direction.IN).getLongId(), 2l);
        assertEquals(edge1.getProperty("weight"), 0.5f);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        edge1.write(out);

        StandardFaunusEdge edge2 = new StandardFaunusEdge(new ModifiableHadoopConfiguration(), new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(edge1, edge2);
        assertEquals(edge2.getLongId(), -1l);
        assertEquals(edge2.getLabel(), "knows");
        assertEquals(edge2.getVertex(Direction.OUT).getLongId(), 1l);
        assertEquals(edge2.getVertex(Direction.IN).getLongId(), 2l);
        assertEquals(edge2.getProperty("weight"), 0.5f);
        assertEquals(edge2.getProperty("type"), "coworker");
        assertEquals(edge2.getProperty("alive"), true);
        assertEquals(edge2.getProperty("bigLong"), Long.MAX_VALUE);
        assertEquals(edge2.getProperty("age"), 1);
        assertEquals(edge2.getPropertyKeys().size(), 5);

    }
}
