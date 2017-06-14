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

import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.serializer.attributes.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;

import static org.junit.Assert.*;

import java.util.HashMap;

/**
 *  @author Scott McQuillan (scott.mcquillan@uk.ibm.com)
 */
public class CustomHashMapSerializer {

	StandardJanusGraph graph;

    @Before
    public void initialize() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS, HashMap.class.getName(), "attribute1");
        config.set(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS, THashMapSerializer.class.getName(), "attribute1");
        
        graph = (StandardJanusGraph) JanusGraphFactory.open(config);
    }

    @After
    public void shutdown() {
        graph.close();
    }

    @Test
    public void testCustomHashMapSerialization() {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        mgmt.makePropertyKey("hashMap").cardinality(Cardinality.SINGLE).dataType(HashMap.class).make();
        mgmt.buildIndex("byTime",Vertex.class).addKey(time).buildCompositeIndex();
        mgmt.makeVertexLabel("person").make();
        mgmt.commit();

        HashMap<String, Object> hashMapIn = new HashMap<String, Object>();
        hashMapIn.put("property1", "value1");
        
        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex v = tx.addVertex("person");
        v.property("time", 5);
        v.property("hashMap", hashMapIn);
        tx.commit();

        tx = graph.newTransaction();
        v = (JanusGraphVertex) tx.query().has("time",5).vertices().iterator().next();
        assertEquals(5,(int)v.value("time"));
        
        HashMap<String, Object> hashMapOut = v.<HashMap<String, Object>>property("hashMap").orElse(null);
        assertNotNull(hashMapOut);
        assertEquals(2, hashMapOut.size());
        assertEquals("value1", hashMapOut.get("property1"));
        assertTrue(hashMapOut.containsKey(THashMapSerializer.class.getName())); // THashMapSerializer adds this

        tx.rollback();
    }
}
