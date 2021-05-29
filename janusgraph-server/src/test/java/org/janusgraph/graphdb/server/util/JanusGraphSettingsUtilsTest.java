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

package org.janusgraph.graphdb.server.util;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.janusgraph.graphdb.server.JanusGraphSettings;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JanusGraphSettingsUtilsTest {

    @Test
    public void testSetDefaultSerializers() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-without-serializers.yaml");

        assertEquals(5, settings.serializers.size());
    }

    @Test
    public void testDontOverwriteSerializers() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-with-serializers.yaml");

        assertEquals(11, settings.serializers.size());
    }

    @Test
    public void testSetDefaultSerializersWithGraphBinaryWithRegistry() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-without-serializers.yaml");

        Optional<Settings.SerializerSettings> graphBinary = settings.serializers
            .stream()
            .filter(it -> it.className.equals("org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1") &&
                it.config.get("ioRegistries") != null).findFirst();

        assertTrue(graphBinary.isPresent());
        assertTrue(((List)graphBinary.get().config.get("ioRegistries")).contains("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"));
    }

    @Test
    public void testSetDefaultSerializersWithGraphBinaryWithResultToString() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-without-serializers.yaml");

        Optional<Settings.SerializerSettings> graphBinary = settings.serializers
            .stream()
            .filter(it -> it.className.equals("org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1"))
            .skip(1).findFirst();

        assertTrue(graphBinary.isPresent());
        assertTrue((boolean) graphBinary.get().config.get("serializeResultToString"));
    }

    @Test
    public void testSetDefaultSerializersWithGryoWithRegistry() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-without-serializers.yaml");

        Optional<Settings.SerializerSettings> gryo = settings.serializers
            .stream()
            .filter(it -> it.className.equals("org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0") &&
                it.config.get("ioRegistries") != null).findFirst();

        assertTrue(gryo.isPresent());
        assertTrue(((List)gryo.get().config.get("ioRegistries")).contains("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"));
    }

    @Test
    public void testSetDefaultSerializersWithGryoWithResultToString() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-without-serializers.yaml");

        Optional<Settings.SerializerSettings> graphBinary = settings.serializers
            .stream()
            .filter(it -> it.className.equals("org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0"))
            .skip(1).findFirst();

        assertTrue(graphBinary.isPresent());
        assertTrue((boolean) graphBinary.get().config.get("serializeResultToString"));
    }

    @Test
    public void testSetDefaultSerializersWithGraphSONWithRegistry() throws Exception {
        Settings settings = JanusGraphSettings.read("src/test/resources/janusgraph-server-without-serializers.yaml");

        Optional<Settings.SerializerSettings> graphson = settings.serializers
            .stream()
            .filter(it -> it.className.equals("org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0") &&
                it.config.get("ioRegistries") != null).findFirst();

        assertTrue(graphson.isPresent());
        assertTrue(((List)graphson.get().config.get("ioRegistries")).contains("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"));
    }
}
