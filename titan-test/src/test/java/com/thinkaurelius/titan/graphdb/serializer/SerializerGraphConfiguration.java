package com.thinkaurelius.titan.graphdb.serializer;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.serializer.attributes.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SerializerGraphConfiguration {

    StandardTitanGraph graph;

    @Before
    public void initialize() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS, TClass1.class.getName(), "attribute1");
        config.set(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS, TClass1Serializer.class.getName(), "attribute1");
        config.set(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS, TEnum.class.getName(), "attribute4");
        config.set(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS, TEnumSerializer.class.getName(), "attribute4");
        graph = (StandardTitanGraph) TitanFactory.open(config);
    }

    @After
    public void shutdown() {
        graph.close();
    }

    @Test
    public void testOnlyRegisteredSerialization() {
        TitanManagement mgmt = graph.openManagement();
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        PropertyKey any  = mgmt.makePropertyKey("any").cardinality(Cardinality.LIST).dataType(Object.class).make();
        mgmt.buildIndex("byTime",Vertex.class).addKey(time).buildCompositeIndex();
        EdgeLabel knows = mgmt.makeEdgeLabel("knows").make();
        VertexLabel person = mgmt.makeVertexLabel("person").make();
        mgmt.commit();

        TitanTransaction tx = graph.newTransaction();
        TitanVertex v = tx.addVertex("person");
        v.property("time", 5);
        v.property("any", new Double(5.0));
        v.property("any", new TClass1(5,1.5f));
        v.property("any", TEnum.THREE);
        tx.commit();

        tx = graph.newTransaction();
        v = tx.query().has("time",5).vertices().iterator().next();
        assertEquals(5,(int)v.value("time"));
        assertEquals(3, Iterators.size(v.properties("any")));
        tx.rollback();

        //Verify that non-registered objects aren't allowed
        for (Object o : new Object[]{new TClass2("abc",5)}) {
            tx = graph.newTransaction();
            v = tx.addVertex("person");
            try {
                v.property("any", o); //Should not be allowed
                tx.commit();
                fail();
            } catch (IllegalArgumentException e) {
            } finally {
                if (tx.isOpen()) tx.rollback();
            }

        }
    }


}
