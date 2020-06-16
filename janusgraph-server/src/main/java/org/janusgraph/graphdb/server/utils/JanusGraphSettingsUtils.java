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

package org.janusgraph.graphdb.server.utils;

import org.apache.tinkerpop.gremlin.server.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JanusGraphSettingsUtils {

    /**
     * Add a default set of serializers, if they are empty.
     *
     * @param settings Parsed Gremlin Server {@link Settings}.
     * @return Update Gremlin Server {@link Settings}.
     */
    public static Settings configureDefaultSerializersIfNotSet(Settings settings) {
        if(settings.serializers.size() != 0)
            return settings;

        List<Settings.SerializerSettings> serializers = new ArrayList<>();

        addSerializerWithRegistry(
            serializers,
            "org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1"
        );
        addSerializerWithResultToString(
            serializers,
            "org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1"
        );

        addSerializerWithRegistry(
            serializers,
            "org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0"
        );
        addSerializerWithResultToString(
            serializers,
            "org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0"
        );

        addSerializerWithRegistry(
            serializers,
            "org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0"
        );

        settings.serializers = serializers;

        return settings;
    }

    private static void addSerializerWithRegistry(List<Settings.SerializerSettings> serializers, String className) {
        Settings.SerializerSettings newSerializerSettings = new Settings.SerializerSettings();
        newSerializerSettings.className = className;
        newSerializerSettings.config = Collections.singletonMap("ioRegistries", Collections.singletonList("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"));
        serializers.add(newSerializerSettings);
    }

    private static void addSerializerWithResultToString(List<Settings.SerializerSettings> serializers, String className) {
        Settings.SerializerSettings newSerializerSettings = new Settings.SerializerSettings();
        newSerializerSettings.className = className;
        newSerializerSettings.config = Collections.singletonMap("serializeResultToString", true);
        serializers.add(newSerializerSettings);
    }

}
