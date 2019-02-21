// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.gremlin.server.auth;

import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator.CONFIG_CREDENTIALS_DB;

public class ConfigBuilder {

    protected final Map<String, Object> config;

    protected ConfigBuilder() {
        config = new HashMap<>();
        config.put(CONFIG_CREDENTIALS_DB, "configCredDb");
    }

    public static ConfigBuilder build() {
        return new ConfigBuilder();
    }

    public ConfigBuilder defaultUser(String defaultUser) {
        config.put(HMACAuthenticator.CONFIG_DEFAULT_USER, defaultUser);
        return this;
    }

    public ConfigBuilder defaultPassword(String defaultPassword) {
        config.put(HMACAuthenticator.CONFIG_DEFAULT_PASSWORD, defaultPassword);
        return this;
    }

    public Map<String, Object> create() {
        return config;
    }
}
