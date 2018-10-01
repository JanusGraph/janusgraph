// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.graphdb.management;

import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.core.schema.JanusGraphManagement;
import static org.janusgraph.core.schema.SchemaStatus.ENABLED;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.PropertyKey;

import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.commons.configuration.MapConfiguration;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;
import static org.junit.Assert.*;

public class ConfigurationManagementGraphTest {

    @Test
    public void shouldReindexIfPropertyKeyExists() throws Exception {
        final JanusGraphManager gm = new JanusGraphManager(new Settings());
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        final MapConfiguration config = new MapConfiguration(map);
        final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfiguration(new CommonsConfiguration(config)));

        final String propertyKeyName = "Created_Using_Template";
        final Class dataType = Boolean.class;
        JanusGraphManagement management = graph.openManagement();
        final PropertyKey key = management.makePropertyKey(propertyKeyName).dataType(dataType).make();
        management.commit();

        // Instantiate the ConfigurationManagementGraph Singleton
        // This is purposefully done after a property key is created to ensure that a REDINDEX is initiated
        new ConfigurationManagementGraph(graph);

        management = graph.openManagement();
        final JanusGraphIndex index = management.getGraphIndex("Created_Using_Template_Index");
        final PropertyKey propertyKey = management.getPropertyKey("Created_Using_Template");
        assertNotNull(index);
        assertNotNull(propertyKey);
        assertEquals(ENABLED, index.getIndexStatus(propertyKey));
        management.commit();
    }
}
