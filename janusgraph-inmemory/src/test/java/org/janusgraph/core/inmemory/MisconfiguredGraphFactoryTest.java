// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core.inmemory;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.management.ConfigurationManagementGraph;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.management.utils.ConfigurationManagementGraphSettingException;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_CUSTOM_VERTEX_ID_TYPES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This class is dedicated to test a common configuration error people consistently make
 * when they aim to use ConfiguredGraphFactory: their management graph config contains
 * `graph.set-vertex-id=true` which leads to runtime error because ConfigurationManagementGraph
 * is managed by JanusGraph internally and doesn't support custom ID
 */
public class MisconfiguredGraphFactoryTest {

    private static JanusGraphManager gm;

    protected MapConfiguration getManagementConfig() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");

        // management graph config is not allowed to use custom vertex id
        map.put(ALLOW_SETTING_VERTEX_ID.toStringWithoutRoot(), "true");
        map.put(ALLOW_CUSTOM_VERTEX_ID_TYPES.toStringWithoutRoot(), "true");
        return ConfigurationUtil.loadMapConfiguration(map);
    }

    @Test
    public void shouldRejectInvalidConfig() throws Exception {
        gm = new JanusGraphManager(new Settings());
        final MapConfiguration config = getManagementConfig();
        final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));

        // Cannot instantiate the ConfigurationManagementGraph Singleton because custom vertex ID is not allowed
        assertThrows(ConfigurationManagementGraphSettingException.class, () -> new ConfigurationManagementGraph(graph));
    }
}

