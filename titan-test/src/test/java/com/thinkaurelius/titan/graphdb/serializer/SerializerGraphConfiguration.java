package com.thinkaurelius.titan.graphdb.serializer;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Precision;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Vertex;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SerializerGraphConfiguration {

    StandardTitanGraph graph;

    @Before
    public void initialize() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(GraphDatabaseConfiguration.ATTRIBUTE_ALLOW_ALL_SERIALIZABLE,false);
        graph = (StandardTitanGraph) TitanFactory.open(config);
    }

    @After
    public void shutdown() {
        graph.shutdown();
    }

    @Test
    public void testOnlyRegisteredSerialization() {
        TitanManagement mgmt = graph.getManagementSystem();
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        PropertyKey any  = mgmt.makePropertyKey("any").dataType(Object.class).make();
        mgmt.buildIndex("byTime",Vertex.class).addKey(time).buildCompositeIndex();
        EdgeLabel knows = mgmt.makeEdgeLabel("knows").make();
        VertexLabel person = mgmt.makeVertexLabel("person").make();
        mgmt.commit();

        TitanTransaction tx = graph.newTransaction();
        TitanVertex v = tx.addVertexWithLabel("person");
        v.setProperty("time",5);
        v.setProperty("any",new Precision(5.0));
        tx.commit();

        tx = graph.newTransaction();
        v = (TitanVertex)Iterables.getOnlyElement(tx.query().has("time",5).vertices());
        assertEquals(5.0, v.<Precision>getProperty("any").doubleValue(),0.0);
        tx.rollback();

        tx = graph.newTransaction();
        v = tx.addVertexWithLabel("person");
        v.setProperty("any",TestEnum.One); //Should not be allowed
        try {
            tx.commit();
            fail();
        } catch (Exception e) {

        }


    }


}
