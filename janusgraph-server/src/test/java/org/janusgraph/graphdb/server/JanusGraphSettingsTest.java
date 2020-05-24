// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.server;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.util.DefaultJanusGraphManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class JanusGraphSettingsTest {

    @Test
    public void testAutoImportEnsureJanusGraphGremlinPluginIsIncluded() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server.yaml");

        assertNotNull(
            settings.scriptEngines.get("gremlin-groovy")
                .plugins
                .get("org.janusgraph.graphdb.tinkerpop.plugin.JanusGraphGremlinPlugin")
        );
    }

    @Test
    public void testAutoImportEnsureGraphManagerIsDefaultJanusGraphManager() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server.yaml");

        assertEquals(DefaultJanusGraphManager.class.getName(), settings.graphManager);
    }

    @Test
    public void testAutoImportJanusGraphManagerIsNotOverwritten() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-cfg.yaml");

        assertEquals(JanusGraphManager.class.getName(), settings.graphManager);
    }

    @Test
    public void testAutoImportEnsureGraphBinaryMessageSerializerV1IsFullyConfiguredWithJanusGraphIoRegistry() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server.yaml");

        Optional<Settings.SerializerSettings> graphBinary = settings.serializers
            .stream()
            .filter(it -> it.className.equals("org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1") &&
                it.config.get("ioRegistries") != null).findFirst();

        assertTrue(graphBinary.isPresent());
        assertTrue(((List)graphBinary.get().config.get("ioRegistries")).contains("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"));
    }

    @Test
    public void testAutoImportEnsureGryoMessageSerializerV3d0IsFullyConfiguredWithJanusGraphIoRegistry() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server.yaml");

        Optional<Settings.SerializerSettings> gryo = settings.serializers
            .stream()
            .filter(it -> it.className.equals("org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0") &&
                it.config.get("ioRegistries") != null).findFirst();

        assertTrue(gryo.isPresent());
        assertTrue(((List)gryo.get().config.get("ioRegistries")).contains("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"));
    }

    @Test
    public void testAutoImportEnsureGraphSONMessageSerializerV3d0IsFullyConfiguredWithJanusGraphIoRegistry() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server.yaml");

        Optional<Settings.SerializerSettings> graphson = settings.serializers
            .stream()
            .filter(it -> it.className.equals("org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0") &&
                it.config.get("ioRegistries") != null).findFirst();

        assertTrue(graphson.isPresent());
        assertTrue(((List)graphson.get().config.get("ioRegistries")).contains("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"));
    }
}
