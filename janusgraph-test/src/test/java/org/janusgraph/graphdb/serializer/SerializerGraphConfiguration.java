// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.serializer;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.serializer.attributes.TClass1;
import org.janusgraph.graphdb.serializer.attributes.TClass1Serializer;
import org.janusgraph.graphdb.serializer.attributes.TClass2;
import org.janusgraph.graphdb.serializer.attributes.TEnum;
import org.janusgraph.graphdb.serializer.attributes.TEnumSerializer;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SerializerGraphConfiguration {

    StandardJanusGraph graph;

    @BeforeEach
    public void initialize() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS, TClass1.class.getName(), "attribute1");
        config.set(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS, TClass1Serializer.class.getName(), "attribute1");
        config.set(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS, TEnum.class.getName(), "attribute4");
        config.set(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS, TEnumSerializer.class.getName(), "attribute4");
        graph = (StandardJanusGraph) JanusGraphFactory.open(config);
    }

    @AfterEach
    public void shutdown() {
        graph.close();
    }

    @Test
    public void testOnlyRegisteredSerialization() {
        JanusGraphManagement management = graph.openManagement();
        PropertyKey time = management.makePropertyKey("time").dataType(Integer.class).make();
        management.makePropertyKey("any").cardinality(Cardinality.LIST).dataType(Object.class).make();
        management.buildIndex("byTime",Vertex.class).addKey(time).buildCompositeIndex();
        management.makeEdgeLabel("knows").make();
        management.makeVertexLabel("person").make();
        management.commit();

        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex v = tx.addVertex("person");
        v.property("time", 5);
        v.property("any", 5.0);
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
            } catch (IllegalArgumentException ignored) {
            } finally {
                if (tx.isOpen()) tx.rollback();
            }

        }
    }


}
