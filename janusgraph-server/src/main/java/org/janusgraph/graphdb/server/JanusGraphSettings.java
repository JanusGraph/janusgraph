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
import org.janusgraph.graphdb.server.util.JanusGraphSettingsUtils;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Objects;

public class JanusGraphSettings extends Settings {
    private JanusGraphSettings.GrpcServerSettings grpcServer = new JanusGraphSettings.GrpcServerSettings();

    /**
     * Whether the Gremlin pool, which evaluates requests, runs them on virtual threads instead of platform threads.
     * Requires Java 24 or later. The pool keeps the bounds of Gremlin Server's own pool: {@link #gremlinPool} requests
     * run at once and up to {@link #maxWorkQueueSize} more wait, beyond which requests are rejected.
     */
    private boolean gremlinPoolVirtualThreads = false;

    public static JanusGraphSettings read(final String file) throws Exception {
        InputStream input = new FileInputStream(file);
        return read(input);
    }

    public static JanusGraphSettings read(final InputStream stream) {
        Objects.requireNonNull(stream);
        Constructor constructor = new Constructor(JanusGraphSettings.class, new LoaderOptions());
        TypeDescription grpcServerSettings = new TypeDescription(JanusGraphSettings.GrpcServerSettings.class);
        constructor.addTypeDescription(grpcServerSettings);

        Yaml yaml = new Yaml(constructor);
        JanusGraphSettings settings = yaml.loadAs(stream, JanusGraphSettings.class);
        return JanusGraphSettingsUtils.configureDefaults(settings);
    }

    public GrpcServerSettings getGrpcServer() {
        return grpcServer;
    }

    public void setGrpcServer(GrpcServerSettings grpcServer) {
        this.grpcServer = grpcServer;
    }

    public boolean isGremlinPoolVirtualThreads() {
        return gremlinPoolVirtualThreads;
    }

    public void setGremlinPoolVirtualThreads(boolean gremlinPoolVirtualThreads) {
        this.gremlinPoolVirtualThreads = gremlinPoolVirtualThreads;
    }

    public static class GrpcServerSettings {
        private boolean enabled = false;
        private int port = 10182;

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
