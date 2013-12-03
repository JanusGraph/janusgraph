package com.thinkaurelius.titan.graphdb.types;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.inmemory.InMemoryStorageAdapter;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.types.reference.TypeReferenceContainer;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

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

        assertTrue(types.containsType(SystemKey.VertexState.getName()));
        assertEquals(SystemKey.TypeDefinition,types.getExistingType(SystemKey.TypeDefinition.getID()));
        assertEquals(SystemKey.TypeClass,types.getType(SystemKey.TypeClass.getName()));
    }

    @Test
    public void testContainerReadingFromGraph() {
        defineTypes(graph);
        graph.commit();

        TypeReferenceContainer types = new TypeReferenceContainer(graph);
        graph.shutdown();
        graph=null;

        verifyTypes(types);

        BaseConfiguration config = new BaseConfiguration();
        types.exportToConfiguration(config);
//        printConfiguration(config);

        TypeReferenceContainer types2 = new TypeReferenceContainer(config);
        verifyTypes(types2);

    }



    private void printConfiguration(Configuration config) {
        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            System.out.println(key + ": " + config.getProperty(key).toString());
        }
    }


}
