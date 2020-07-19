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
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.representer.Representer;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

public class JanusGraphSettings {
    public JanusGraphSettings.GrpcServerSettings grpcServer = new JanusGraphSettings.GrpcServerSettings();

    private Settings gremlinSettings;

    public static class GrpcServerSettings {
        public boolean enable = false;
        public int port = 10182;
    }

    public Settings getGremlinSettings() {
        return gremlinSettings;
    }

    public static JanusGraphSettings read(final String file) throws Exception {
        InputStream input = new FileInputStream(file);
        return read(input);
    }

    public static JanusGraphSettings read(final InputStream stream) throws IOException {
        Objects.requireNonNull(stream);
        Constructor constructor = new Constructor(JanusGraphSettings.class);
        TypeDescription grpcServerSettings = new TypeDescription(JanusGraphSettings.GrpcServerSettings.class);
        constructor.addTypeDescription(grpcServerSettings);
        Representer representer = new Representer();
        representer.getPropertyUtils().setSkipMissingProperties(true);

        String yamlInput = getStringFromInputStream(stream);

        Yaml yaml = new Yaml(constructor, representer);
        JanusGraphSettings settings = yaml.loadAs(yamlInput, JanusGraphSettings.class);
        Yaml yamlMap = new Yaml();
        Map<String, Object> obj = yamlMap.load(yamlInput);
        obj.remove("grpcServer");
        String dump = yamlMap.dump(obj);
        settings.gremlinSettings = Settings.read(new ByteArrayInputStream(dump.getBytes()));
        return settings;
    }

    private static String getStringFromInputStream(InputStream stream) throws IOException {
        int ch;
        StringBuilder sb = new StringBuilder();
        while ((ch = stream.read()) != -1)
            sb.append((char) ch);
        return sb.toString();
    }
}
