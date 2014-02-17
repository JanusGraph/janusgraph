package com.thinkaurelius.titan.graphdb.types;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.reference.TypeReferenceContainer;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonWriter;
import java.io.File;
import java.io.StringWriter;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TypeReferenceContainerTest {

    private TitanGraph graph;

    @Before
    public void setup() {
        graph = StorageSetup.getInMemoryGraph();
    }

    @After
    public void tearDown() {
        if (graph!=null) graph.shutdown();
    }


    private void defineTypes(TitanGraph graph) {
        graph.makeLabel("label1").manyToOne().make();
        TitanKey time = graph.makeKey("time").dataType(Long.class).single(TypeMaker.UniquenessConsistency.NO_LOCK).make();
        assertFalse(((InternalType)time).uniqueLock(Direction.OUT));
        graph.makeLabel("posted").manyToMany().sortKey(time).sortOrder(Order.DESC).make();
        graph.makeKey("uid").dataType(String.class).indexed(Vertex.class).indexed(Edge.class).unique(TypeMaker.UniquenessConsistency.LOCK).single(TypeMaker.UniquenessConsistency.NO_LOCK).make();
    }

    private void verifyTypes(TypeInspector types) {
        assertTrue(types.containsType("label1"));
        TitanLabel l = (TitanLabel)types.getType("label1");
        assertEquals("label1",l.getName());
        assertTrue(l.isDirected());
        assertFalse(l.isUnique(Direction.IN));
        assertTrue(l.isUnique(Direction.OUT));
        assertTrue(l.isEdgeLabel());
        InternalType t = (InternalType)l;
        assertFalse(t.uniqueLock(Direction.IN));
        assertTrue(t.uniqueLock(Direction.OUT));
        assertEquals(0,t.getSortKey().length);
        assertEquals(l,types.getExistingType(l.getID()));

        TitanKey time = (TitanKey)types.getType("time");
        assertEquals(Long.class,time.getDataType());
        assertEquals(0, Iterables.size(time.getIndexes(Vertex.class)));
        InternalType posted = (InternalType)types.getType("posted");
        assertEquals(1,posted.getSortKey().length);
        assertEquals(time.getID(),posted.getSortKey()[0]);
        assertEquals(Order.DESC,posted.getSortOrder());
        TitanKey uid = (TitanKey)types.getType("uid");
        assertEquals(uid,types.getExistingType(uid.getID()));
        assertEquals(1,Iterables.size(uid.getIndexes(Vertex.class)));
        assertEquals(1,Iterables.size(uid.getIndexes(Edge.class)));
        assertFalse(((InternalType) uid).uniqueLock(Direction.OUT));
        assertTrue(((InternalType) uid).uniqueLock(Direction.IN));
        assertEquals(String.class,uid.getDataType());

        assertTrue(types.containsType(SystemKey.VertexExists.getName()));
        assertEquals(SystemKey.TypeDefinition,types.getExistingType(SystemKey.TypeDefinition.getID()));
        assertEquals(SystemKey.TypeCategory,types.getType(SystemKey.TypeCategory.getName()));
    }

    @Test
    public void testContainerReadingFromGraph() {
        defineTypes(graph);
        graph.commit();

        TypeReferenceContainer types = new TypeReferenceContainer(graph);
        graph.shutdown();
        graph=null;

        verifyTypes(types);

        JsonArray config = types.exportToJson();
        printConfiguration(config);

        TypeReferenceContainer types2 = new TypeReferenceContainer(config);
        verifyTypes(types2);

        String filename = "target" + File.separator + "schema.json";
        types.exportToFile(filename);

        TypeReferenceContainer types3 = new TypeReferenceContainer(filename);
        verifyTypes(types3);

        FileUtils.deleteQuietly(new File(filename));

        TitanGraph graph2 = StorageSetup.getInMemoryGraph();
        types3.installInGraph(graph2);
        verifyTypes((StandardTitanTx)graph2.newTransaction());
        graph2.shutdown();

    }



    private void printConfiguration(JsonArray config) {
        StringWriter s = new StringWriter();
        JsonWriter writer = Json.createWriter(s);
        writer.writeArray(config);
        writer.close();
        System.out.println(s.toString());
    }


}
