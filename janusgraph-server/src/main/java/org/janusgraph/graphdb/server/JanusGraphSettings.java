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
import org.apache.tinkerpop.gremlin.server.util.DefaultGraphManager;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.util.DefaultJanusGraphManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JanusGraphSettings {
    public static Settings read(final String file) throws Exception {
        InputStream input = new FileInputStream(new File(file));
        Settings read = Settings.read(input);
        return autoImport(read);
    }
    private static Settings autoImport(Settings settings) {
        settings.scriptEngines.get("gremlin-groovy")
            .plugins
            .putIfAbsent("org.janusgraph.graphdb.tinkerpop.plugin.JanusGraphGremlinPlugin", Collections.emptyMap());

        setSerializer(
            settings,
            "org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1",
            "org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"
        );
        setSerializer(
            settings,
            "org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0",
            "org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"
        );
        setSerializer(
            settings,
            "org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0",
            "org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"
        );

        if (DefaultGraphManager.class.getName().equals(settings.graphManager)) {
            settings.graphManager = DefaultJanusGraphManager.class.getName();
        }

        return settings;
    }

    private static void setSerializer(Settings settings, String className, String registryClassName) {
        Optional<Settings.SerializerSettings> serializerSettings = settings.serializers.stream()
            .filter(it -> it.className.equals(className) && it.config.get("ioRegistries") != null)
            .findFirst();
        if (!serializerSettings.isPresent()) {
            Settings.SerializerSettings newSerializerSettings = new Settings.SerializerSettings();
            newSerializerSettings.className = className;
            newSerializerSettings.config = Collections.singletonMap("ioRegistries", Collections.singletonList(registryClassName));
            settings.serializers.add(newSerializerSettings);
        } else {
            List<String> ioRegistries = (List<String>)serializerSettings.get().config.putIfAbsent("ioRegistries", Collections.emptyList());
            if (ioRegistries != null && !ioRegistries.contains(registryClassName)) {
                Stream<String> stream = ioRegistries.stream().map(it -> it);
                Set<String> toMutableList = stream.collect(Collectors.toSet());
                toMutableList.add(registryClassName);
                serializerSettings.get().config.put("ioRegistries", toMutableList);
            }
        }
    }

}
